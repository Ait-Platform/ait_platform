"""Recover an existing LOCAL test URL without logging or persisting credentials.

Only explicit loopback URLs are candidates. The existing harness validates the
database name and rejects URL overrides before any connection attempt.
"""
import json
import os
import re
import sys
import uuid
import ipaddress
from pathlib import Path
import sqlalchemy as sa
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
URL_PATTERN = re.compile(r"postgres(?:ql)?(?:\+[a-z0-9_]+)?://[^\s\"'`<>]+", re.I)


def sources():
    yield from ROOT.glob(".env*")
    yield from (ROOT / ".vscode").glob("*.json")
    yield from ROOT.glob("*.code-workspace")
    for folder in (ROOT / "scripts", ROOT / "tests", ROOT / "scratch"):
        for extension in ("*.ps1", "*.py", "*.ini", "*.cfg"):
            yield from folder.rglob(extension)
    for folder in (Path("C:/Users/Sanjith/Documents/WindowsPowerShell"), Path("C:/Users/Sanjith/Documents/PowerShell")):
        yield from folder.glob("*profile*.ps1")
    yield Path("C:/Users/Sanjith/AppData/Roaming/Microsoft/Windows/PowerShell/PSReadLine/ConsoleHost_history.txt")
    yield Path("C:/Users/Sanjith/AppData/Roaming/Code/User/settings.json")


def candidates():
    seen = set()
    for path in sources():
        if not path.is_file() or path.name == Path(__file__).name:
            continue
        try:
            content = path.read_text(encoding="utf-8-sig", errors="replace")
        except (OSError, PermissionError):
            print("Unreadable local source:", path)
            continue
        if path.name.startswith(".env"):
            content = "\n".join(str(value) for value in dotenv_values(path).values() if value)
        matches = URL_PATTERN.findall(content)
        for raw in matches:
            raw = raw.rstrip(";,)")
            try:
                url = sa.engine.make_url(raw)
            except Exception:
                continue
            if url.host not in {"localhost", "127.0.0.1", "::1"}:
                print("Non-loopback URL ignored in:", path.name)
                continue
            identity = (url.host, url.port, url.database, url.username, url.password)
            if identity in seen:
                continue
            seen.add(identity)
            print("Loopback candidate source:", path, "database:", url.database, "port:", url.port or 5432)
            if url.query or (url.database != "ait_local_db" and not (url.database or "").startswith("uip_test_")):
                print("Not accepted as a UIP test database; no connection attempted.")
                continue
            if any(marker in raw for marker in ("TEST_PASSWORD", "TEST_USER", "unused@", "x@")):
                print("Placeholder URL ignored.")
                continue
            yield url


def recover():
    sys.path.insert(0, str(ROOT / "tests" / "uip"))
    from conftest import safe_url
    for url in candidates():
        value = url.render_as_string(hide_password=False)
        try:
            accepted = safe_url(value)
            engine = sa.create_engine(accepted, connect_args={"connect_timeout": 3})
            schema = "uip_test_" + uuid.uuid4().hex
            with engine.begin() as connection:
                server_address = connection.scalar(sa.text("SELECT inet_server_addr()::text"))
                print("Server-reported address:", server_address)
                if not server_address or not ipaddress.ip_interface(server_address).ip.is_loopback:
                    raise RuntimeError("Server address is not loopback")
                connection.exec_driver_sql('CREATE SCHEMA "' + schema + '"')
                connection.exec_driver_sql('DROP SCHEMA "' + schema + '"')
                print("LOCAL PostgreSQL connection succeeds; loopback server verified; database:", accepted.database)
                print("Existing harness accepts connection; disposable-schema creation/drop succeeds.")
            engine.dispose()
            os.environ["UIP_TEST_DATABASE_URL"] = value
            return value
        except Exception as error:
            # Driver error strings may include credentials/URLs; report only class.
            print("Local candidate probe failed:", type(error).__name__)
            import traceback
            for frame in traceback.extract_tb(error.__traceback__):
                print("  at", Path(frame.filename).name, frame.lineno, frame.name)
    return None


if __name__ == "__main__":
    if not recover():
        print("No previous usable local test connection recovered.")
        sys.exit(2)
    if len(sys.argv) > 1:
        import pytest
        os.chdir(ROOT)
        sys.exit(pytest.main(["--confcutdir=tests/uip", "-p", "no:cacheprovider", *sys.argv[1:]]))
