"""HOME manual builder foundation: reproducible source snapshots, no final PDFs.

Run with --snapshot OUTPUT.json to export the verified local HOME content read-only.
A production-parity/source approval and print renderer are required before final manuals.
This script never starts the application, invokes participant routes or changes DB data.
"""
import argparse
import importlib.util
import json
from pathlib import Path
from dotenv import dotenv_values
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    args = parser.parse_args()
    url = make_url(dotenv_values(ROOT / ".env")["DATABASE_URL"])
    if (url.get_backend_name() != "postgresql" or url.database != "ait_local_db"
            or url.host not in ("localhost", "127.0.0.1", "::1") or url.query):
        parser.error("Snapshot builder requires verified localhost ait_local_db without URL overrides.")
    spec = importlib.util.spec_from_file_location("home_manual_sources", ROOT / "app/program_sace_home/manual_sources.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            connection = connection.execution_options(isolation_level="REPEATABLE READ", postgresql_readonly=True)
            with connection.begin():
                snapshot = module.source_snapshot(connection, ROOT)
        args.snapshot.write_text(json.dumps(snapshot, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
        print("HOME source snapshot:", snapshot["source_sha256"])
        print("Content gaps:", len(snapshot["gaps"]))
        for gap in snapshot["gaps"]:
            print("-", gap)
        print("Not approved for final manuals; production parity remains unchecked.")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
