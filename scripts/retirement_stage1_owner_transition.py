"""Local-only ownership-cardinality transition. No factory or migration replay."""
import hashlib
import json
import sys
import sqlalchemy as sa
import retirement_stage2_local as prepared

TABLES = ('retirement_organisation', 'retirement_membership', 'retirement_role')


def snapshot(conn):
    result = {}
    for table in TABLES:
        rows = conn.execute(sa.text('SELECT to_jsonb(t) FROM public.' + table + ' t ORDER BY id')).scalars().all()
        result[table] = (len(rows), hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest())
    return result


def transition():
    engine = prepared.local_engine()  # Rejects remote targets and inherited production URLs.
    with engine.begin() as conn:
        prepared.identity(conn)
        print('Verified intended LOCAL PostgreSQL: ait_local_db / public / loopback.', flush=True)
        conn.execute(sa.text('LOCK TABLE public.retirement_organisation IN ACCESS EXCLUSIVE MODE'))
        conn.execute(sa.text('LOCK TABLE public.retirement_membership, public.retirement_role IN SHARE ROW EXCLUSIVE MODE'))
        before = snapshot(conn)
        revision = conn.execute(sa.text('SELECT version_num FROM public.alembic_version')).scalars().all()
        uniques = sa.inspect(conn).get_unique_constraints('retirement_organisation', schema='public')
        assert not uniques or (len(uniques) == 1 and uniques[0]['column_names'] == ['owner_user_id']), 'Unexpected uniqueness dependency'
        prepared.stage1(conn, legacy_owner_unique=bool(uniques))
        constraints_before = conn.execute(sa.text("SELECT oid, conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=ANY(ARRAY['public.retirement_organisation'::regclass,'public.retirement_membership'::regclass,'public.retirement_role'::regclass]) ORDER BY oid")).all()
        indexes_before = conn.execute(sa.text("SELECT indexname,indexdef FROM pg_indexes WHERE schemaname='public' AND tablename=ANY(:tables) ORDER BY indexname"), {'tables':list(TABLES)}).all()
        dropped_oid = None
        dropped_index = None
        if uniques:
            name = uniques[0]['name']
            row = conn.execute(sa.text("SELECT c.oid, i.relname FROM pg_constraint c JOIN pg_class i ON i.oid=c.conindid WHERE c.conrelid='public.retirement_organisation'::regclass AND c.conname=:name AND c.contype='u'"), {'name':name}).one()
            dropped_oid, dropped_index = row
            foreign_dependencies = conn.execute(sa.text("SELECT count(*) FROM pg_constraint WHERE contype='f' AND conindid=(SELECT conindid FROM pg_constraint WHERE oid=:oid)"), {'oid':dropped_oid}).scalar_one()
            assert foreign_dependencies == 0, 'Unexpected foreign-key dependency; stop'
            quoted = conn.dialect.identifier_preparer.quote(name)
            conn.execute(sa.text('ALTER TABLE public.retirement_organisation DROP CONSTRAINT ' + quoted + ' RESTRICT'))
        # A separate unique index could silently preserve the old limitation.
        assert not [i for i in sa.inspect(conn).get_indexes('retirement_organisation', schema='public') if i['unique']], 'Unexpected unique index'
        prepared.stage1(conn)
        constraints_after = conn.execute(sa.text("SELECT oid, conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=ANY(ARRAY['public.retirement_organisation'::regclass,'public.retirement_membership'::regclass,'public.retirement_role'::regclass]) ORDER BY oid")).all()
        assert constraints_after == [r for r in constraints_before if r[0] != dropped_oid]
        indexes_after = conn.execute(sa.text("SELECT indexname,indexdef FROM pg_indexes WHERE schemaname='public' AND tablename=ANY(:tables) ORDER BY indexname"), {'tables':list(TABLES)}).all()
        assert indexes_after == [r for r in indexes_before if r[0] != dropped_index]
        assert snapshot(conn) == before, 'Retirement data changed; rollback'
        assert conn.execute(sa.text('SELECT version_num FROM public.alembic_version')).scalars().all() == revision
        print('All Retirement rows, IDs, history and remaining constraints/indexes preserved.', flush=True)
        print('Precommit counts:', {t:v[0] for t,v in before.items()}, flush=True)
    print('COMMITTED: owner uniqueness removed.' if dropped_oid else 'VERIFIED NO-OP: already transitioned.')
    engine.dispose()


if __name__ == '__main__':
    try:
        transition()
    except Exception as error:
        print('Transition stopped; transaction rolled back. Error type: ' + type(error).__name__)
        sys.exit(1)
