"""Explicit LOCAL-only additive Waiting Room transition; no app factory/migrations."""
import itertools
from pathlib import Path
import types
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.schema import CreateTable, CreateIndex, AddConstraint
import retirement_stage2_local as legacy

NEW = ('retirement_waiting_user', 'retirement_association_review')
MEMBER = 'retirement_membership'
EXTRA_CHECKS = {'ck_retirement_membership_association_pair', 'ck_retirement_membership_association_only'}
TRIGGER = 'retirement_association_review_immutable'
FUNCTION = 'retirement_association_review_reject_mutation'


def definitions():
    # Evaluate only this program's declarative definitions, with no app startup.
    db = types.SimpleNamespace(Model=declarative_base(), relationship=relationship)
    for name in ('Column', 'Integer', 'String', 'Text', 'Boolean', 'DateTime', 'ForeignKey',
                 'UniqueConstraint', 'CheckConstraint', 'Index', 'func', 'text'):
        setattr(db, name, getattr(sa, name))
    source = (legacy.ROOT / 'app/models/retire.py').read_text(encoding='utf-8')
    source = source.replace('from app.extensions import db', '')
    exec(compile(source, 'retirement-model-definitions', 'exec'), {'db':db})
    metadata = db.Model.metadata
    sa.Table('user', metadata, sa.Column('id', sa.Integer, primary_key=True))
    return metadata


def snapshot(conn):
    return {name: conn.execute(sa.text('SELECT to_jsonb(t) FROM public.' + name + ' t ORDER BY id')).scalars().all()
            for name in ('retirement_organisation', MEMBER, 'retirement_role')}


def verify_table(conn, table):
    ins = sa.inspect(conn); name = table.name
    columns = ins.get_columns(name, schema='public')
    assert [(c['name'], c['type'].compile(dialect=conn.dialect), c['nullable']) for c in columns] == [(c.name, c.type.compile(dialect=conn.dialect), c.nullable) for c in table.columns], name + ' columns'
    assert ins.get_pk_constraint(name, schema='public')['constrained_columns'] == ['id']
    assert sorted(tuple(u['column_names']) for u in ins.get_unique_constraints(name,schema='public')) == sorted(tuple(c.name for c in u.columns) for u in table.constraints if isinstance(u,sa.UniqueConstraint))
    fks = ins.get_foreign_keys(name,schema='public')
    assert sorted((tuple(f['constrained_columns']), f['referred_table'], tuple(f['referred_columns'])) for f in fks) == sorted(((f.parent.name,), f.column.table.name, (f.column.name,)) for f in table.foreign_keys)
    assert all(f['referred_schema'] in (None,'public') and not f['options'] for f in fks)
    assert sorted((i['name'],tuple(i['column_names']),i['unique']) for i in ins.get_indexes(name,schema='public') if not i.get('duplicates_constraint')) == sorted((i.name,tuple(c.name for c in i.columns),i.unique) for i in table.indexes)
    checks = ins.get_check_constraints(name,schema='public')
    expected = {c.name:str(c.sqltext) for c in table.constraints if isinstance(c,sa.CheckConstraint)}
    assert {c['name'] for c in checks} == set(expected)
    if name == MEMBER:
        samples = [dict(status=s, approved_role_id=a, requested_role_id=r, association_approved_at=t,
                        association_approved_by_user_id=u) for s,a,r,t,u in itertools.product(
                        (None,'pending','active','denied','disabled','invalid'), (None,1), (None,1),
                        (None,'2026-01-01T00:00:00Z'), (None,1))]
    elif name == NEW[0]:
        samples = [dict(preferred_name=s) for s in ('', ' ', 'Mary')]
    elif name == NEW[1]:
        samples = [dict(decision=d, version=v) for d,v in itertools.product(('approve','not_ours','invalid'),(-1,0,1,2))]
    else:
        samples = []
    for row in samples:
        projection = ','.join('CAST(:' + k + ' AS ' + table.c[k].type.compile(dialect=conn.dialect) + ') AS ' + k for k in row)
        for check in checks:
            query = 'SELECT ('+check['sqltext']+') IS NOT DISTINCT FROM ('+expected[check['name']]+') FROM (SELECT '+projection+') s'
            assert conn.execute(sa.text(query),row).scalar_one(), check['name']
    for column in columns:
        if column['name'] == 'id':
            legacy.sequence_check(conn,name)
        elif column['name'] == 'requested_at':
            assert column['default'].lower() in ('now()','current_timestamp','transaction_timestamp()')
        else:
            assert column['default'] is None


def verify(conn):
    legacy.identity(conn); legacy.stage1(conn)
    metadata = definitions()
    for name in ('retirement_role', MEMBER) + NEW:
        verify_table(conn, metadata.tables[name])
    module, _ = legacy.migration_source()
    assert set(conn.execute(sa.text('SELECT code,name FROM public.retirement_role')).all()) == set(module.ROLES)
    assert conn.execute(sa.text("""SELECT count(*) FROM public.retirement_organisation o
        LEFT JOIN public.retirement_membership m ON m.organisation_id=o.id AND m.user_id=o.owner_user_id
        LEFT JOIN public.retirement_role r ON r.id=m.approved_role_id
        WHERE m.id IS NULL OR m.status IS DISTINCT FROM 'active'
        OR r.code IS DISTINCT FROM 'organisation_owner' OR m.requested_role_id IS NOT NULL""")).scalar_one() == 0
    row = conn.execute(sa.text("SELECT t.tgtype,t.tgenabled,p.proname,p.prosrc FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgrelid='public.retirement_association_review'::regclass AND t.tgname=:name"), {'name':TRIGGER}).one()
    assert row[0] == 27 and row[1] == 'O' and row[2] == FUNCTION and row[3].strip() == FUNCTION_BODY.strip()
    return {n:conn.execute(sa.text('SELECT count(*) FROM public.'+n)).scalar_one() for n in ('retirement_organisation',MEMBER,'retirement_role')+NEW}


FUNCTION_BODY = "BEGIN RAISE EXCEPTION 'Retirement association review history is immutable'; END;"


def transition():
    engine = legacy.local_engine()
    with engine.begin() as conn:
        legacy.identity(conn)
        print('Verified LOCAL ait_local_db / public / loopback before writes.',flush=True)
        metadata = definitions()
        present = [sa.inspect(conn).has_table(n,schema='public') for n in NEW]
        if any(present):
            assert all(present), 'Partial Waiting Room schema; stop'
            counts = verify(conn)
            print('VERIFIED NO-OP:',counts,flush=True)
            return
        module, old = legacy.definitions()
        legacy.verify(conn,old,module)
        assert not conn.execute(sa.text("SELECT 1 FROM pg_trigger WHERE tgrelid='public.retirement_membership'::regclass AND NOT tgisinternal")).first(), 'Unexpected membership trigger'
        assert not conn.execute(sa.text("SELECT 1 FROM pg_constraint WHERE contype='f' AND confrelid='public.retirement_membership'::regclass")).first(), 'Unexpected incoming membership dependency'
        assert not conn.execute(sa.text("SELECT 1 FROM pg_depend d JOIN pg_rewrite r ON d.classid='pg_rewrite'::regclass AND r.oid=d.objid WHERE d.refobjid='public.retirement_membership'::regclass")).first(), 'Unexpected view dependency'
        assert not conn.execute(sa.text("SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND (c.relname LIKE 'retirement_waiting_%' OR c.relname LIKE 'retirement_association_%' OR c.relname LIKE 'uq_retirement_association_%')")).first(), 'Naming collision'
        assert not conn.execute(sa.text("SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.proname=:name"),{'name':FUNCTION}).first(), 'Function collision'
        conn.execute(sa.text('LOCK TABLE public.retirement_organisation, public.retirement_membership, public.retirement_role IN ACCESS EXCLUSIVE MODE'))
        before = snapshot(conn)
        revision = conn.execute(sa.text('SELECT version_num FROM public.alembic_version')).scalars().all()
        conn.execute(sa.text('ALTER TABLE public.retirement_membership ADD COLUMN association_approved_at TIMESTAMP WITH TIME ZONE, ADD COLUMN association_approved_by_user_id INTEGER REFERENCES public."user"(id), ALTER COLUMN status DROP NOT NULL'))
        for constraint in metadata.tables[MEMBER].constraints:
            if constraint.name in EXTRA_CHECKS:
                conn.execute(AddConstraint(constraint))
        for name in NEW:
            conn.execute(CreateTable(metadata.tables[name]))
            for index in metadata.tables[name].indexes:
                conn.execute(CreateIndex(index))
        conn.execute(sa.text('CREATE FUNCTION public.'+FUNCTION+"() RETURNS trigger LANGUAGE plpgsql AS $$"+FUNCTION_BODY+'$$'))
        conn.execute(sa.text('CREATE TRIGGER '+TRIGGER+' BEFORE UPDATE OR DELETE ON public.retirement_association_review FOR EACH ROW EXECUTE FUNCTION public.'+FUNCTION+'()'))
        after = snapshot(conn)
        after[MEMBER] = [{k:v for k,v in row.items() if k not in ('association_approved_at','association_approved_by_user_id')} for row in after[MEMBER]]
        assert before == after, 'Existing Retirement rows changed'
        assert revision == conn.execute(sa.text('SELECT version_num FROM public.alembic_version')).scalars().all()
        counts = verify(conn)
        print('Verified preserved rows/IDs/history:', {k:len(v) for k,v in before.items()},flush=True)
    print('COMMITTED local Waiting Room transition:',counts,flush=True)
    engine.dispose()


if __name__ == '__main__':
    import sys
    try:
        if sys.argv[1:] == ['apply']:
            transition()
        elif sys.argv[1:] == ['verify']:
            engine = legacy.local_engine()
            with engine.connect() as conn:
                conn.exec_driver_sql('SET TRANSACTION READ ONLY')
                print('VERIFIED:',verify(conn))
        else:
            raise ValueError('Specify apply or verify')
    except Exception as error:
        print('STOPPED / transaction rolled back:',type(error).__name__)
        raise SystemExit(1)
