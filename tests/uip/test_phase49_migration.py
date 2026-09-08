import importlib.util
import pytest
import sqlalchemy as sa
from alembic.config import Config
from alembic.script import ScriptDirectory
from bootstrap import ROOT
from conftest import migrate_phase3, migrate_phase49
from test_bridge import fingerprint


def test_phase49_ancestry_and_legacy_head_preserved():
    graph = ScriptDirectory.from_config(Config(str(ROOT / "alembic.ini")))
    assert [r.revision for r in graph.iterate_revisions("uip_p49_operations", "base")] == [
        "uip_p49_operations", "uip_p3_work_orders", "a27c9e4b6102", "uip_p2_prod_base"]
    assert set(graph.get_heads()) == {"uip_p49_operations", "7da57fffdba9"}


def test_phase49_preserves_existing_data_and_protected_schema(phase2_engine):
    with phase2_engine.connect() as connection:
        transaction = connection.begin()
        try:
            migrate_phase3(connection)
            connection.exec_driver_sql('INSERT INTO "user" (id,name) VALUES (990101,\'Local synthetic\')')
            connection.exec_driver_sql("INSERT INTO core_organization (id,name,slug) VALUES (990101,'Synthetic','synthetic-p49')")
            connection.exec_driver_sql("INSERT INTO core_interaction (id,organization_id,creator_id,title,interaction_type) VALUES (990101,990101,990101,'Preserve','SECURITY')")
            connection.exec_driver_sql("INSERT INTO uip_municipal_referral (id,interaction_id,department,status) VALUES (990101,990101,'Water','ESCALATED')")
            connection.exec_driver_sql("INSERT INTO uip_committee_meeting (id,organization_id,title,scheduled_at,status) VALUES (990101,990101,'Historical','2025-01-01','CONCLUDED')")
            connection.exec_driver_sql("INSERT INTO uip_resolution (id,meeting_id,title,status) VALUES (990101,990101,'Existing','APPROVED')")
            connection.exec_driver_sql("INSERT INTO uip_document (id,organization_id,uploader_id,filename) VALUES (990101,990101,990101,'legacy.pdf')")
            connection.exec_driver_sql("INSERT INTO core_audit_event (id,action,details) VALUES (990101,'PROTECTED','UNCHANGED')")
            tables = sa.inspect(connection).get_table_names()
            columns = {t: [c["name"] for c in sa.inspect(connection).get_columns(t)] for t in tables}
            def rows():
                return {t: connection.exec_driver_sql('SELECT ' + ','.join('"' + c + '"' for c in columns[t]) +
                    ' FROM "' + t + '" t ORDER BY to_jsonb(t)::text').all() for t in tables}
            before = rows()
            protected = {t: fingerprint(connection, t) for t in tables if not t.startswith("uip_")}
            migrate_phase49(connection)
            assert rows() == before
            assert {t: fingerprint(connection, t) for t in protected} == protected
            assert connection.exec_driver_sql("SELECT organization_id FROM uip_municipal_referral WHERE id=990101").scalar() == 990101
            assert connection.exec_driver_sql("SELECT organization_id FROM uip_resolution WHERE id=990101").scalar() == 990101
            assert connection.exec_driver_sql("SELECT quorum_achieved FROM uip_committee_meeting WHERE id=990101").scalar() is None
            for table in ("uip_sla_clock", "uip_document_version", "uip_survey_response", "uip_referral_event"):
                assert connection.exec_driver_sql("SELECT count(*) FROM " + table).scalar() == 0
        finally:
            transaction.rollback()


def test_phase49_downgrade_refuses_history_loss():
    spec = importlib.util.spec_from_file_location("p49", ROOT / "migrations/versions/uip_p49_operations.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with pytest.raises(RuntimeError, match="history"):
        module.downgrade()


def test_phase49_append_only_triggers_present(engine):
    with engine.connect() as connection:
        for name in ("uip_referral_event_immutable", "uip_document_version_immutable",
                     "uip_survey_response_immutable", "uip_decision_event_immutable", "uip_resolution_immutable",
                     "uip_meeting_frozen", "uip_survey_frozen"):
            assert connection.execute(sa.text("SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE t.tgname=:name AND n.nspname=current_schema()"), {"name": name}).scalar() == 1
