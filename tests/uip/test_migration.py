import ast
import runpy
import pytest
import sqlalchemy as sa
from alembic.config import Config
from alembic import context
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from bootstrap import db, core, uip, ROOT, NEW_TABLES
from conftest import migrate, revision, safe_url
from test_register import make_member


def test_revision_parent_and_model_schema_parity(engine):
    assert revision().revision == "a27c9e4b6102"
    assert revision().down_revision == "uip_p2_prod_base"
    with engine.connect() as connection:
        inspector=sa.inspect(connection)
        assert set(NEW_TABLES) <= set(inspector.get_table_names())
        diffs=compare_metadata(MigrationContext.configure(connection, opts={"compare_type":True}), db.metadata)
        assert diffs == []


def test_empty_downgrade_and_reupgrade(phase2_engine):
    with phase2_engine.connect() as connection:
        transaction=connection.begin()
        try:
            migrate(connection,"downgrade")
            assert not set(NEW_TABLES) & set(sa.inspect(connection).get_table_names())
            assert not next(c for c in sa.inspect(connection).get_columns("core_organization_member") if c["name"]=="user_id")["nullable"]
            migrate(connection)
            assert set(NEW_TABLES) <= set(sa.inspect(connection).get_table_names())
        finally:
            transaction.rollback()


def test_populated_downgrade_refused(data):
    make_member(data);db.session.commit()
    with pytest.raises(sa.exc.DBAPIError, match="preservation plan"), db.session.begin_nested():
        migrate(db.session.connection(),"downgrade")
    assert uip.UipMemberProfile.query.count()==1


def test_migration_does_not_change_existing_records(phase2_engine):
    with phase2_engine.connect() as connection:
        transaction=connection.begin()
        try:
            migrate(connection,"downgrade")
            connection.execute(sa.text("INSERT INTO core_organization(id,name,slug) VALUES (900000,'Legacy','legacy')"))
            connection.execute(sa.text('INSERT INTO "user"(id,name,email) VALUES (:id,:name,:email)'), dict(id=900000,name="Legacy",email="legacy@example.invalid"))
            connection.execute(sa.text("INSERT INTO core_organization_member(id,organization_id,user_id,is_active) VALUES(900000,900000,900000,true)"))
            connection.execute(sa.text("INSERT INTO core_audit_event(id,action,details) VALUES(900000,'SACE_SENTINEL','Untouched')"))
            tables=("core_organization","user","core_organization_member","core_audit_event")
            before={t:connection.execute(sa.text('SELECT * FROM "'+t+'"')).all() for t in tables}
            migrate(connection)
            after={t:connection.execute(sa.text('SELECT * FROM "'+t+'"')).all() for t in tables}
            assert before==after
        finally:
            transaction.rollback()


def test_runner_uses_supplied_connection_without_factory(engine,monkeypatch):
    import sys
    monkeypatch.setattr(sys.modules["app"],"create_app",lambda:pytest.fail("Factory must never execute"),raising=False)
    config=Config()
    with engine.connect() as connection:
        config.attributes["connection"]=connection
        monkeypatch.setattr(context,"config",config,raising=False)
        monkeypatch.setattr(context,"is_offline_mode",lambda:False)
        calls=[]
        monkeypatch.setattr(context,"configure",lambda **kw:calls.append(kw))
        from contextlib import nullcontext
        monkeypatch.setattr(context,"begin_transaction",nullcontext)
        monkeypatch.setattr(context,"run_migrations",lambda:None)
        runpy.run_path(str(ROOT/"migrations/env.py"))
        assert calls[0]["connection"] is connection
        assert "uip_audit_event" in calls[0]["target_metadata"].tables


@pytest.mark.parametrize("value", ["postgresql://x@example.com/ait_local_db", "sqlite://", "postgresql://x@localhost/production", "postgresql://x@localhost/ait_local_db?host=example.com", None])
def test_harness_refuses_unsafe_database(value):
    with pytest.raises(RuntimeError):
        safe_url(value)


def test_real_metadata_offline_runner_never_starts_app_or_connects():
    import os
    import subprocess
    import sys
    script = r"""
import io, sys
from sqlalchemy.engine import Engine
from alembic.config import Config
from alembic import command

def forbidden(*args, **kwargs):
    raise AssertionError("Database connection forbidden in offline metadata test")
Engine.connect = forbidden
Engine.raw_connection = forbidden

def guard(frame, event, arg):
    if event == 'call' and frame.f_code.co_name == 'create_app':
        raise AssertionError('Application factory forbidden in migration runner')
sys.setprofile(guard)
config = Config()
config.set_main_option('script_location', 'migrations')
config.output_buffer = io.StringIO()
command.upgrade(config, 'uip_p2_prod_base:a27c9e4b6102', sql=True)
output = config.output_buffer.getvalue()
assert 'CREATE TABLE uip_audit_event' in output
assert 'CREATE TABLE uip_member_profile' in output
assert 'DROP TABLE' not in output
assert 'UPDATE user_enrollment' not in output
print('Actual metadata loaded and scoped offline migration compiled without factory or connection')
"""
    environment = dict(os.environ, DATABASE_URL="postgresql://unused@127.0.0.1/uip_test_offline", SQLALCHEMY_DATABASE_URI="postgresql://unused@127.0.0.1/uip_test_offline")
    result=subprocess.run([sys.executable,"-B","-c",script], cwd=ROOT, env=environment, capture_output=True,text=True,timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
