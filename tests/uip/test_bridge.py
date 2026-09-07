"""Production prerequisite reconstruction from the read-only review, not a full dump."""
import importlib.util
import json
import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.migration import MigrationContext
from alembic.operations import Operations
from bootstrap import ROOT, NEW_TABLES
from conftest import migrate


def bridge():
    spec=importlib.util.spec_from_file_location("uip_baseline",ROOT/"migrations/versions/uip_p2_prod_base.py")
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def validate(connection):
    with Operations.context(MigrationContext.configure(connection)):
        bridge().upgrade()


@pytest.fixture
def production_baseline(phase2_engine):
    with phase2_engine.connect() as connection:
        transaction=connection.begin()
        try:
            migrate(connection,"downgrade")
            # Preserve the inspected production mismatch, defaults and missing optional FKs.
            connection.execute(sa.text("ALTER TABLE core_organization_member ALTER COLUMN is_active DROP NOT NULL, ALTER COLUMN is_active SET DEFAULT true"))
            connection.execute(sa.text("ALTER TABLE core_organization ALTER COLUMN status SET DEFAULT 'active'"))
            connection.execute(sa.text('ALTER TABLE "user" ALTER COLUMN name TYPE varchar, ALTER COLUMN email TYPE varchar, ALTER COLUMN email SET NOT NULL'))
            for fk in sa.inspect(connection).get_foreign_keys("core_interaction"):
                if fk["constrained_columns"] in (["assigned_to"],["closed_by"]):
                    name=connection.dialect.identifier_preparer.quote(fk["name"])
                    connection.execute(sa.text("ALTER TABLE core_interaction DROP CONSTRAINT " + name))
            # Protected SACE sentinel structures mirror the local SACE model.
            connection.execute(sa.text("""CREATE TABLE sace_documents (
                id serial PRIMARY KEY, slug varchar(50) NOT NULL, document_type varchar(50) NOT NULL,
                file_name varchar(255) NOT NULL, file_path varchar(255) NOT NULL,
                uploaded_at timestamp, uploaded_by integer REFERENCES "user"(id) ON DELETE SET NULL)"""))
            connection.execute(sa.text("""CREATE TABLE sace_workshop_interactions (
                id serial PRIMARY KEY, user_id integer NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
                workshop_session_id varchar(50), activity_slug varchar(50) NOT NULL,
                response_data text NOT NULL, timestamp timestamp)"""))
            connection.execute(sa.text('INSERT INTO "user"(id,name,email) VALUES (900000,:name,:email)'),dict(name="Existing",email="existing@example.invalid"))
            connection.execute(sa.text("INSERT INTO core_organization(id,name,slug) VALUES(900000,'Existing','existing')"))
            connection.execute(sa.text("INSERT INTO core_organization_member(id,organization_id,user_id,is_active) VALUES(900000,900000,900000,true)"))
            connection.execute(sa.text("INSERT INTO core_interaction(id,organization_id,creator_id,reference,interaction_type,title) VALUES(900000,900000,900000,'LEGACY','SECURITY','Existing issue')"))
            connection.execute(sa.text("INSERT INTO core_audit_event(id,action,details) VALUES(900000,'SACE_SENTINEL','Do not change')"))
            connection.execute(sa.text("INSERT INTO sace_workshop_interactions(id,user_id,activity_slug,response_data) VALUES(900000,900000,'existing','Do not change')"))
            yield connection
        finally:
            transaction.rollback()


def fingerprint(connection, table):
    inspector=sa.inspect(connection)
    return json.dumps(dict(columns=inspector.get_columns(table),pk=inspector.get_pk_constraint(table),
        fks=inspector.get_foreign_keys(table),indexes=inspector.get_indexes(table),
        unique=inspector.get_unique_constraints(table)),default=str,sort_keys=True)


def test_accepts_inspected_baseline_without_writes(production_baseline):
    connection=production_baseline
    statements=[]
    def capture(conn,cursor,statement,parameters,context,many):
        statements.append(statement)
    sa.event.listen(connection,"before_cursor_execute",capture)
    try:
        validate(connection)
    finally:
        sa.event.remove(connection,"before_cursor_execute",capture)
    assert statements and all(s.lstrip().upper().startswith("SELECT") for s in statements)
    assert next(c for c in sa.inspect(connection).get_columns("core_organization_member") if c["name"]=="is_active")["nullable"]


@pytest.mark.parametrize("ddl,match", [
    ("ALTER TABLE core_task RENAME TO missing_core_task", "missing prerequisite tables"),
    ("ALTER TABLE core_interaction DROP COLUMN channel", "missing prerequisite column"),
    ("ALTER TABLE core_interaction ALTER COLUMN title TYPE text", "unexpected type"),
    ("ALTER TABLE core_organization_member ALTER COLUMN user_id DROP NOT NULL", "unexpected nullable"),
    ("DROP INDEX ix_core_interaction_reference", "missing unique key"),
])
def test_rejects_missing_or_incompatible_prerequisites(production_baseline,ddl,match):
    production_baseline.execute(sa.text(ddl))
    with pytest.raises(RuntimeError,match=match):
        validate(production_baseline)


@pytest.mark.parametrize("name",NEW_TABLES)
def test_rejects_partial_phase2_tables(production_baseline,name):
    production_baseline.execute(sa.text('CREATE TABLE "'+name+'" (id integer)'))
    with pytest.raises(RuntimeError,match="Phase 2 relations already exist"):
        validate(production_baseline)


@pytest.mark.parametrize("name",("member_id","property_id","recorded_by"))
def test_rejects_partial_phase2_columns(production_baseline,name):
    production_baseline.execute(sa.text('ALTER TABLE core_interaction ADD COLUMN "'+name+'" integer'))
    with pytest.raises(RuntimeError,match="Phase 2 core_interaction columns already exist"):
        validate(production_baseline)


@pytest.mark.parametrize("ddl,match",[
    ("CREATE SEQUENCE uip_member_profile_id_seq","Phase 2 relations"),
    ("ALTER TABLE core_organization_member ADD CONSTRAINT uq_core_member_id_org UNIQUE(id,organization_id)","Phase 2 relations"),
    ("CREATE FUNCTION uip_audit_reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$","Phase 2 audit function"),
])
def test_rejects_partial_phase2_other_objects(production_baseline,ddl,match):
    production_baseline.execute(sa.text(ddl))
    with pytest.raises(RuntimeError,match=match):validate(production_baseline)


def test_null_membership_status_requires_review(production_baseline):
    production_baseline.execute(sa.text("UPDATE core_organization_member SET is_active=NULL"))
    with pytest.raises(RuntimeError,match="contains NULL"):validate(production_baseline)


def test_only_uip_ancestry():
    config=Config(str(ROOT/"alembic.ini"))
    scripts=ScriptDirectory.from_config(config)
    ancestors=list(scripts.iterate_revisions("a27c9e4b6102","base"))
    assert [r.revision for r in ancestors]==["a27c9e4b6102","uip_p2_prod_base"]
    assert ancestors[1].down_revision is None
    assert "uip" in ancestors[1].branch_labels
    assert set(scripts.get_heads())=={"uip_p3_work_orders","7da57fffdba9"}


def test_real_graph_upgrade_preserves_business_data_and_protected_structures(production_baseline):
    connection=production_baseline
    tables=sa.inspect(connection).get_table_names()
    before_schema={t:fingerprint(connection,t) for t in tables}
    old_columns={t:[c["name"] for c in sa.inspect(connection).get_columns(t)] for t in tables}
    quote=connection.dialect.identifier_preparer.quote
    def rows(table):
        cols=", ".join(quote(c) for c in old_columns[table])
        return connection.execute(sa.text("SELECT "+cols+" FROM "+quote(table)+" ORDER BY 1")).all()
    before_data={t:rows(t) for t in tables}
    config=Config()
    config.set_main_option("script_location",str(ROOT/"migrations"))
    config.attributes["connection"]=connection
    command.upgrade(config,"a27c9e4b6102")
    assert connection.execute(sa.text("SELECT version_num FROM alembic_version")).scalars().all()==["a27c9e4b6102"]
    assert set(sa.inspect(connection).get_table_names())-set(tables)==set(NEW_TABLES)|{"alembic_version"}
    for table in tables:
        assert rows(table)==before_data[table],table
        if table not in ("core_interaction","core_organization_member"):
            assert fingerprint(connection,table)==before_schema[table],table
    assert set(c["name"] for c in sa.inspect(connection).get_columns("core_interaction"))-set(old_columns["core_interaction"])=={"member_id","property_id","recorded_by"}
    membership={c["name"]:c for c in sa.inspect(connection).get_columns("core_organization_member")}
    assert membership["user_id"]["nullable"] and membership["is_active"]["nullable"]
    assert connection.scalar(sa.text("SELECT count(*) FROM uip_audit_event"))==0
    assert connection.scalar(sa.text("SELECT count(*) FROM pg_trigger WHERE tgrelid='uip_audit_event'::regclass AND tgname='uip_audit_append_only'"))==1


def test_baseline_downgrade_refuses_to_undo_adoption(production_baseline):
    with Operations.context(MigrationContext.configure(production_baseline)):
        with pytest.raises(RuntimeError,match="cannot be automatically undone"):
            bridge().downgrade()


def test_offline_baseline_refused():
    with Operations.context(MigrationContext.configure(dialect_name="postgresql",opts={"as_sql":True})):
        with pytest.raises(RuntimeError,match="online catalog validation"):
            bridge().upgrade()


def test_unexpected_migration_history_rejected(production_baseline):
    production_baseline.execute(sa.text("CREATE TABLE alembic_version(version_num varchar(32) NOT NULL PRIMARY KEY)"))
    production_baseline.execute(sa.text("INSERT INTO alembic_version VALUES ('7da57fffdba9')"))
    with pytest.raises(RuntimeError,match="expected empty migration history"):
        validate(production_baseline)


def test_empty_production_version_table_accepted(production_baseline):
    production_baseline.execute(sa.text("CREATE TABLE alembic_version(version_num varchar(32) NOT NULL PRIMARY KEY)"))
    validate(production_baseline)
    assert production_baseline.scalar(sa.text("SELECT count(*) FROM alembic_version"))==0
