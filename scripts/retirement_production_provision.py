"""Explicit Retirement-only production provisioning; no application startup."""
import os
import sys
import json
import sqlalchemy as sa
import psycopg2
from sqlalchemy.schema import CreateTable, CreateIndex
import retirement_stage2_local as prepared


def identity(conn):
    row = tuple(conn.execute(sa.text('SELECT current_database(), session_user, current_user')).one())
    assert row == ('ait_platform_db', 'aitplatformdb_retirement', 'ait_platform_db_user'), 'Identity mismatch'
    return row


def main():
    engine = sa.create_engine('postgresql+psycopg2://', creator=lambda: psycopg2.connect(os.environ['RENDER_DATABASE_URL'], connect_timeout=20))
    module, metadata = prepared.definitions()
    # Reuse the prepared schema verifier, replacing its LOCAL-only identity and
    # revision assumptions. Never execute any migration entry point.
    prepared.identity = identity
    prepared.revision = lambda conn: None
    with engine.begin() as conn:
        identity(conn)
        conn.execute(sa.text('SET LOCAL search_path = public'))
        conn.execute(sa.text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(sa.text("SET LOCAL statement_timeout = '30s'"))
        inspector = sa.inspect(conn)
        user_columns = inspector.get_columns('user', schema='public')
        uid = next(c for c in user_columns if c['name'] == 'id')
        assert str(uid['type']) == 'INTEGER' and not uid['nullable'], 'Incompatible user.id'
        assert inspector.get_pk_constraint('user', schema='public')['constrained_columns'] == ['id']
        assert all(conn.execute(sa.text("SELECT has_schema_privilege(current_user,'public','USAGE'), has_schema_privilege(current_user,'public','CREATE'), has_column_privilege(current_user,'public.\"user\"','id','REFERENCES')")).one())
        existing = conn.execute(sa.text("SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND (c.relname LIKE 'retirement_organisation%' OR c.relname LIKE 'retirement_role%' OR c.relname LIKE 'retirement_membership%' OR c.relname IN ('uq_retirement_membership_org_user','ix_retirement_membership_org_status','ix_retirement_membership_user_id'))")).scalars().all()
        if existing:
            counts = prepared.verify(conn, metadata, module)
            print(json.dumps({'already_provisioned_and_validated': counts}))
            return
        collisions = conn.execute(sa.text("SELECT count(*) FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='public' AND t.typname IN ('retirement_organisation','_retirement_organisation','retirement_role','_retirement_role','retirement_membership','_retirement_membership')")).scalar_one()
        assert collisions == 0, 'Type name collision'
        collisions = conn.execute(sa.text("SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname='public' AND (c.conname LIKE 'retirement_organisation%' OR c.conname LIKE 'retirement_role%' OR c.conname LIKE 'retirement_membership%' OR c.conname IN ('uq_retirement_membership_org_user','ck_retirement_membership_status','ck_retirement_membership_active_role','ck_retirement_membership_unapproved_role'))")).scalar_one()
        assert collisions == 0, 'Constraint name collision'
        print('Stage 1 dependencies, privileges and naming preflight PASSED.', flush=True)
        conn.execute(sa.text('CREATE TABLE public.retirement_organisation (id SERIAL NOT NULL PRIMARY KEY, name VARCHAR(200) NOT NULL, owner_user_id INTEGER NOT NULL, UNIQUE(owner_user_id), FOREIGN KEY(owner_user_id) REFERENCES public."user"(id))'))
        prepared.stage1(conn)
        assert conn.execute(sa.text('SELECT count(*) FROM public.retirement_organisation')).scalar_one() == 0
        print('Stage 1 definition validated; Stage 2 names absent, canonical roles safe and owner backfill empty.', flush=True)
        for name in ('retirement_role', 'retirement_membership'):
            conn.execute(CreateTable(metadata.tables[name]))
            for index in metadata.tables[name].indexes:
                conn.execute(CreateIndex(index))
        ids = module.establish_roles(conn, metadata.tables['retirement_role'])
        module.backfill_owner_memberships(conn, metadata.tables['retirement_organisation'], metadata.tables['retirement_membership'], ids['organisation_owner'])
        counts = prepared.verify(conn, metadata, module)
        print(json.dumps({'precommit_validation_passed': counts}), flush=True)
    print('Retirement-only transaction COMMITTED.', flush=True)
    with engine.connect() as conn:
        print(json.dumps({'postcommit_validation_passed': prepared.verify(conn, metadata, module)}))
    engine.dispose()


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('Provisioning stopped (' + type(error).__name__ + '); sensitive error details suppressed.')
        sys.exit(1)
