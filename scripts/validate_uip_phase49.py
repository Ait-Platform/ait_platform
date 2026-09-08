"""One consolidated local validation; never load the shared application factory."""
import ast
import os
from pathlib import Path
import subprocess
import sys
import uuid
from jinja2 import Environment
from recover_uip_local_test import recover, ROOT


def main():
    if not recover():
        return 2
    os.chdir(ROOT)
    output = []
    failures = []
    commands = (
        ("UIP Phase 1–9 suite (including parity, migrations and security)", [sys.executable, "-B", "-m", "pytest",
            "--confcutdir=tests/uip", "tests/uip", "-q", "-p", "no:cacheprovider", "--tb=short",
            "--basetemp=" + str(ROOT / "scratch" / ("uip_validation_" + uuid.uuid4().hex))]),
        ("Startup preservation", [sys.executable, "-B", "tests/test_startup_enrollment_preservation.py"]),
    )
    for label, command in commands:
        print(label, flush=True)
        result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace")
        text = label + "\n" + result.stdout + result.stderr
        print(result.stdout + result.stderr, flush=True)
        output.append(text)
        if result.returncode:
            failures.append(label)
    python_files = set((ROOT / "app/uip").rglob("*.py")) | set((ROOT / "app/models").glob("uip*.py")) | set((ROOT / "tests/uip").rglob("*.py"))
    python_files.add(ROOT / "migrations/versions/uip_p49_operations.py")
    for file in python_files:
        ast.parse(file.read_text(encoding="utf-8-sig"), filename=str(file))
    templates = list((ROOT / "templates/uip").rglob("*.html"))
    for file in templates:
        Environment().parse(file.read_text(encoding="utf-8-sig"))
    parsed = f"Python/Jinja parsing: {len(python_files)} Python files and {len(templates)} UIP templates passed."
    print(parsed)
    output.append(parsed)
    from alembic.config import Config
    from alembic.script import ScriptDirectory
    graph = ScriptDirectory.from_config(Config(str(ROOT / "alembic.ini")))
    assert set(graph.get_heads()) == {"uip_p49_operations", "7da57fffdba9"}
    chain = " -> ".join(reversed([r.revision for r in graph.iterate_revisions("uip_p49_operations", "base")]))
    print("Migration ancestry:", chain)
    output.append("Migration ancestry: " + chain)
    changed = subprocess.run(["git", "diff", "--name-only", "--", "app"], capture_output=True, text=True, check=True).stdout.splitlines()
    assert all(path.startswith("app/uip/") or path.startswith("app/models/uip") for path in changed), changed
    assert not subprocess.run(["git", "diff", "--name-only", "--", "migrations"], capture_output=True, text=True, check=True).stdout.strip(), "Existing migrations changed"
    for file in (ROOT / "app/uip").rglob("*.py"):
        assert "CoreAuditEvent" not in file.read_text(encoding="utf-8-sig"), file
    isolation = "Product isolation: application changes confined to UIP; existing migrations and CoreAuditEvent untouched."
    print(isolation)
    output.append(isolation)
    log = ROOT / "scratch/uip_completion_validation.txt"
    log.write_text("\n\n".join(output), encoding="utf-8")
    print("Validation log:", log)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
