"""Build a private HOME Auditor bundle from verified localhost educational content only."""
import argparse
import importlib
import sys
import types
from pathlib import Path
from dotenv import dotenv_values
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--approved-by', required=True)
    parser.add_argument('--approval-reference', required=True)
    parser.add_argument('--output-root', type=Path, default=ROOT / 'instance/sace_home_examination')
    args = parser.parse_args()
    url = make_url(dotenv_values(ROOT / '.env')['DATABASE_URL'])
    if url.get_backend_name() != 'postgresql' or url.host not in ('localhost', '127.0.0.1', '::1') or url.database != 'ait_local_db' or url.query:
        parser.error('Only verified localhost ait_local_db without overrides is permitted.')
    # Standalone package prevents importing app/__init__.py and startup maintenance.
    package = types.ModuleType('home_snapshot_tools')
    package.__path__ = [str(ROOT / 'app/program_sace_home')]
    sys.modules[package.__name__] = package
    source = importlib.import_module('home_snapshot_tools.manual_sources')
    content = importlib.import_module('home_snapshot_tools.content_snapshot')
    engine = create_engine(url)
    try:
        with engine.connect().execution_options(isolation_level='REPEATABLE READ', postgresql_readonly=True) as conn:
            with conn.begin():
                inventory = source.source_snapshot(conn, ROOT)
                bundle = content.build(inventory, ROOT, args.approved_by, args.approval_reference)
        target = content.write_bundle(bundle, args.output_root)
        print('Private HOME examination bundle:', target)
        print('Version:', bundle['version'])
        print('Local source only; production parity is not asserted. No final PDFs generated.')
    finally:
        engine.dispose()


if __name__ == '__main__':
    main()
