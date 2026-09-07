import importlib.util
import pytest
import sqlalchemy as sa
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.operations import Operations
from alembic.migration import MigrationContext
from bootstrap import ROOT
from conftest import migrate_phase3
from test_bridge import fingerprint


def test_phase3_ancestry_only_uip():
    scripts=ScriptDirectory.from_config(Config(str(ROOT/"alembic.ini")))
    assert [r.revision for r in scripts.iterate_revisions("uip_p3_work_orders","base")]==["uip_p3_work_orders","a27c9e4b6102","uip_p2_prod_base"]
    assert scripts.get_revision("uip_p3_work_orders").down_revision=="a27c9e4b6102"


def test_phase3_migration_preserves_existing_business_columns(phase2_engine):
    with phase2_engine.connect() as conn:
        tx=conn.begin()
        try:
            conn.exec_driver_sql('INSERT INTO "user"(id,name,email) VALUES (901001,\'Synthetic\',\'synthetic@example.invalid\')')
            conn.exec_driver_sql("INSERT INTO core_organization(id,name,slug) VALUES(901001,'Existing','existing')")
            conn.exec_driver_sql("INSERT INTO core_interaction(id,organization_id,creator_id,title,interaction_type) VALUES(901001,901001,901001,'Original','SECURITY')")
            conn.exec_driver_sql("INSERT INTO core_task(id,interaction_id,title,status) VALUES(901001,901001,'Historical','pending')")
            conn.exec_driver_sql("INSERT INTO uip_provider(id,organization_id,name) VALUES(901001,901001,'Legacy provider')")
            conn.exec_driver_sql("INSERT INTO core_audit_event(id,action,details) VALUES(901001,'SACE_SENTINEL','UNCHANGED')")
            conn.exec_driver_sql("CREATE TABLE sace_phase3_sentinel(id integer PRIMARY KEY, body text)")
            conn.exec_driver_sql("INSERT INTO sace_phase3_sentinel VALUES(1,'UNCHANGED')")
            tables=["user","core_organization","core_interaction","core_task","uip_provider","core_audit_event","sace_phase3_sentinel"]
            columns={t:[c["name"] for c in sa.inspect(conn).get_columns(t)] for t in tables}
            def rows():return {t:conn.exec_driver_sql('SELECT '+','.join('"'+c+'"' for c in columns[t])+' FROM "'+t+'" ORDER BY id').all() for t in tables}
            before=rows();protected={t:fingerprint(conn,t) for t in ("core_audit_event","sace_phase3_sentinel")}
            migrate_phase3(conn)
            assert rows()==before
            assert {t:fingerprint(conn,t) for t in protected}==protected
            assert conn.exec_driver_sql("SELECT availability FROM uip_provider WHERE id=901001").scalar()=="UNKNOWN"
            assert conn.exec_driver_sql("SELECT uip_cancelled_at FROM core_task WHERE id=901001").scalar() is None
            assert {"uip_provider_capability","uip_provider_user","uip_work_order_action"} <= set(sa.inspect(conn).get_table_names())
            assert conn.exec_driver_sql("SELECT count(*) FROM uip_provider_capability").scalar()==0
            assert conn.exec_driver_sql("SELECT count(*) FROM uip_provider_user").scalar()==0
            trigger=conn.exec_driver_sql("SELECT count(*) FROM pg_trigger WHERE tgrelid='uip_work_order_action'::regclass AND tgname='uip_work_order_action_append_only'").scalar()
            assert trigger==1
        finally:tx.rollback()


def test_existing_work_orders_refused_before_any_ddl(phase2_engine):
    with phase2_engine.connect() as conn:
        tx=conn.begin()
        try:
            conn.exec_driver_sql('INSERT INTO "user"(id,name) VALUES(901002,\'Existing\')')
            conn.exec_driver_sql("INSERT INTO core_organization(id,name,slug) VALUES(901002,'Existing','existing')")
            conn.exec_driver_sql("INSERT INTO core_interaction(id,organization_id,creator_id,title,interaction_type) VALUES(901002,901002,901002,'Original','SECURITY')")
            conn.exec_driver_sql("INSERT INTO uip_provider(id,organization_id,name) VALUES(901002,901002,'Legacy')")
            conn.exec_driver_sql("INSERT INTO uip_work_order(interaction_id,provider_id,status) VALUES(901002,901002,'SENT')")
            before=fingerprint(conn,"uip_work_order")
            with pytest.raises(RuntimeError,match="historical preservation"):migrate_phase3(conn)
            assert fingerprint(conn,"uip_work_order")==before
        finally:tx.rollback()


@pytest.mark.parametrize("ddl",["CREATE TABLE uip_provider_user(id integer)","ALTER TABLE core_task ADD COLUMN uip_version integer"])
def test_partial_phase3_refused(phase2_engine,ddl):
    with phase2_engine.connect() as conn:
        tx=conn.begin()
        try:
            conn.exec_driver_sql(ddl)
            with pytest.raises(RuntimeError,match="pre-existing"):migrate_phase3(conn)
        finally:tx.rollback()


def test_phase3_downgrade_refused(engine):
    spec=importlib.util.spec_from_file_location("p3",ROOT/"migrations/versions/uip_p3_work_orders.py")
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    with engine.connect() as conn,Operations.context(MigrationContext.configure(conn)):
        with pytest.raises(RuntimeError,match="preservation plan"):module.downgrade()


def test_exact_alembic_target_on_phase2_database(phase2_engine):
    from alembic import command
    with phase2_engine.connect() as conn:
        tx=conn.begin()
        try:
            conn.exec_driver_sql("CREATE TABLE alembic_version(version_num varchar(32) NOT NULL PRIMARY KEY)")
            conn.exec_driver_sql("INSERT INTO alembic_version VALUES ('a27c9e4b6102')")
            config=Config(str(ROOT/"alembic.ini"))
            config.attributes["connection"]=conn
            command.upgrade(config,"uip_p3_work_orders")
            assert conn.exec_driver_sql("SELECT version_num FROM alembic_version").all()==[("uip_p3_work_orders",)]
        finally:tx.rollback()


def test_frozen_phase2_fixture_matches_migrated_phase2(phase2_engine):
    import uuid
    with phase2_engine.connect() as conn:
        tx=conn.begin()
        try:
            original=conn.exec_driver_sql("SELECT current_schema()").scalar()
            before={t:fingerprint(conn,t) for t in sa.inspect(conn).get_table_names()}
            schema="uip_test_"+uuid.uuid4().hex
            conn.exec_driver_sql('CREATE SCHEMA "'+schema+'"')
            conn.exec_driver_sql('SET LOCAL search_path TO "'+schema+'"')
            conn.exec_driver_sql((ROOT/"tests/uip/fixtures/phase2_schema.sql").read_text())
            after={t:fingerprint(conn,t) for t in sa.inspect(conn).get_table_names()}
            assert before==after
        finally:tx.rollback()
