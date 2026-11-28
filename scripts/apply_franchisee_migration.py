"""Approved atomic franchisee migration for Render pre-deploy execution."""
import os
import sys
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

PARENT = 'protrade_trade_codes_019'
TARGET = 'ait_franchisee_001'
engine = None
stage = 'configuration'
try:
    url = make_url(os.environ['DATABASE_URL'].replace('postgres://', 'postgresql://', 1))
    if url.get_backend_name() != 'postgresql':
        raise RuntimeError('PostgreSQL required')
    config = Config('migrations/alembic.ini')
    config.attributes.update(skip_app_metadata=True, franchisee_registration_approved=True)
    scripts=ScriptDirectory.from_config(config)
    if scripts.get_revision(TARGET).down_revision != PARENT:
        raise RuntimeError('Unexpected migration dependency')
    engine = create_engine(url, poolclass=NullPool, connect_args={'connect_timeout': 10})
    stage = 'database connection'
    with engine.begin() as connection:
        connection.exec_driver_sql("SET LOCAL lock_timeout = '5s'")
        connection.exec_driver_sql("SET LOCAL statement_timeout = '60s'")
        connection.exec_driver_sql("SET LOCAL idle_in_transaction_session_timeout = '90s'")
        before = set(connection.execute(text('SELECT version_num FROM alembic_version')).scalars())
        objects = connection.execute(text("SELECT (to_regclass('ait_franchisee_registration') IS NOT NULL) <> (to_regclass('franchise_registration') IS NOT NULL), to_regclass('ait_franchisee_number_seq') IS NOT NULL")).one()
        ancestors={r.revision for head in before for r in scripts.walk_revisions(base='base',head=head)}
        if TARGET in ancestors:
            if not all(objects):
                raise RuntimeError('Recorded revision and schema disagree')
            expected = before
        else:
            if PARENT not in before or any(objects):
                raise RuntimeError('Parent is not current or unrecorded franchisee objects exist; stop for review')
            expected = (before - {PARENT}) | {TARGET}
        print('Revision heads before:', sorted(before), flush=True)
        config.attributes['connection'] = connection
        stage = 'Alembic upgrade'
        if TARGET not in ancestors:
            command.upgrade(config, TARGET)
        after = set(connection.execute(text('SELECT version_num FROM alembic_version')).scalars())
        objects = connection.execute(text("SELECT (to_regclass('ait_franchisee_registration') IS NOT NULL) <> (to_regclass('franchise_registration') IS NOT NULL), to_regclass('ait_franchisee_number_seq') IS NOT NULL")).one()
        if after != expected or not all(objects):
            raise RuntimeError('Post-upgrade revision/schema check failed')
        stage = 'commit'
    print('COMMITTED. Revision heads after:', sorted(after), flush=True)
except Exception as error:
    code = getattr(getattr(error, 'orig', None), 'pgcode', None) or 'n/a'
    print('FAILED during %s: %s; SQLSTATE=%s. Verify database state before retry.' % (stage, type(error).__name__, code), file=sys.stderr)
    raise SystemExit(1)
finally:
    if engine is not None:
        engine.dispose()
