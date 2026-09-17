"""Local-only Stage 3 schema, evidenced legacy import and strict verification."""
import sys
import sqlalchemy as sa
from sqlalchemy.schema import CreateTable, CreateIndex
import retirement_waiting_room_transition as waiting

TABLES=('retirement_relationship','retirement_staff_role_assignment','retirement_authority_event')
FUNCTION='retirement_authority_integrity'
BODY="""
DECLARE rel public.retirement_relationship%ROWTYPE; mem public.retirement_membership%ROWTYPE; code text;
BEGIN
 IF TG_OP = 'DELETE' OR TG_OP = 'TRUNCATE' THEN RAISE EXCEPTION 'Authority history cannot be deleted'; END IF;
 IF TG_TABLE_NAME = 'retirement_authority_event' THEN
  IF TG_OP <> 'INSERT' THEN RAISE EXCEPTION 'Authority events are immutable'; END IF;
  SELECT * INTO STRICT rel FROM public.retirement_relationship WHERE id=NEW.relationship_id;
  IF rel.membership_id <> NEW.membership_id THEN RAISE EXCEPTION 'Event membership mismatch'; END IF;
  IF (NEW.action LIKE 'role_%') <> (NEW.assignment_id IS NOT NULL) THEN RAISE EXCEPTION 'Event assignment mismatch'; END IF;
  IF NEW.assignment_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.retirement_staff_role_assignment WHERE id=NEW.assignment_id AND relationship_id=NEW.relationship_id) THEN RAISE EXCEPTION 'Event relationship mismatch'; END IF;
  RETURN NEW;
 END IF;
 IF TG_OP = 'UPDATE' THEN
  IF OLD.withdrawn_at IS NOT NULL OR NEW.withdrawn_at IS NULL OR
   (to_jsonb(OLD)-'withdrawn_at'-'withdrawn_by_user_id'-'withdrawal_reason') IS DISTINCT FROM
   (to_jsonb(NEW)-'withdrawn_at'-'withdrawn_by_user_id'-'withdrawal_reason') THEN
    RAISE EXCEPTION 'Grant history is immutable; only first withdrawal is permitted';
  END IF;
 END IF;
 IF TG_TABLE_NAME = 'retirement_relationship' THEN
  SELECT * INTO STRICT mem FROM public.retirement_membership WHERE id=NEW.membership_id FOR UPDATE;
  IF mem.association_approved_at IS NULL THEN RAISE EXCEPTION 'Approved home association required'; END IF;
  IF TG_OP='UPDATE' AND EXISTS (SELECT 1 FROM public.retirement_staff_role_assignment WHERE relationship_id=NEW.id AND withdrawn_at IS NULL) THEN RAISE EXCEPTION 'Close Staff roles before withdrawing relationship'; END IF;
 ELSE
  SELECT * INTO STRICT rel FROM public.retirement_relationship WHERE id=NEW.relationship_id FOR UPDATE;
  SELECT r.code INTO STRICT code FROM public.retirement_role r WHERE r.id=NEW.role_id;
  IF rel.kind <> 'staff' OR rel.withdrawn_at IS NOT NULL OR code NOT IN ('facility_manager','care_staff','administration_reception','finance','kitchen_catering') THEN RAISE EXCEPTION 'Current Staff relationship and operational role required'; END IF;
  IF NEW.legacy_source_membership_id IS NOT NULL AND NEW.legacy_source_membership_id <> rel.membership_id THEN RAISE EXCEPTION 'Legacy source mismatch'; END IF;
 END IF;
 RETURN NEW;
END;
"""


def inventory(conn):
    now=conn.execute(sa.text('SELECT clock_timestamp()')).scalar_one()
    rows=conn.execute(sa.text("""SELECT m.*,r.code,o.owner_user_id FROM public.retirement_membership m
        JOIN public.retirement_organisation o ON o.id=m.organisation_id
        LEFT JOIN public.retirement_role r ON r.id=m.approved_role_id WHERE m.status='active' ORDER BY m.id""")).mappings().all()
    eligible=[];blocked=[]
    for row in rows:
        if row['user_id']==row['owner_user_id'] and row['code']=='organisation_owner':continue
        if row['code'] not in ('facility_manager','care_staff','administration_reception','finance','kitchen_catering') or row['reviewed_at'] is None or row['reviewed_by_user_id']!=row['owner_user_id'] or not (row['review_reason'] or '').strip() or row['reviewed_at'] < row['requested_at'] or row['reviewed_at'] > now:
            blocked.append(row['id'])
        else:eligible.append(row)
    if blocked:raise ValueError('Reconciliation blocker: active membership IDs '+','.join(map(str,blocked)))
    return eligible


def import_legacy(conn):
    """Caller owns the transaction; existing period means never re-import/revive."""
    rows=inventory(conn);metadata=waiting.definitions();rels=metadata.tables[TABLES[0]];assigns=metadata.tables[TABLES[1]];events=metadata.tables[TABLES[2]]
    imported=0
    for row in rows:
        conn.execute(sa.text('SELECT id FROM public.retirement_membership WHERE id=:id FOR UPDATE'),{'id':row['id']})
        old=conn.execute(sa.select(rels).where(rels.c.legacy_source_membership_id==row['id'])).mappings().first()
        if old:
            assert conn.execute(sa.select(assigns.c.id).where(assigns.c.legacy_source_membership_id==row['id'],assigns.c.relationship_id==old['id'])).first(), 'Partial legacy import'
            continue
        if conn.execute(sa.select(rels.c.id).where(rels.c.membership_id==row['id'])).first():
            raise ValueError('Reconcile existing relationship before legacy import')
        if row['association_approved_at'] is None:
            conn.execute(sa.text('UPDATE public.retirement_membership SET association_approved_at=:at,association_approved_by_user_id=:by WHERE id=:id'),{'at':row['reviewed_at'],'by':row['reviewed_by_user_id'],'id':row['id']})
        now=conn.execute(sa.text('SELECT clock_timestamp()')).scalar_one()
        evidence=dict(granted_at=row['reviewed_at'],granted_by_user_id=row['reviewed_by_user_id'],recorded_at=now,origin='legacy_import',legacy_source_membership_id=row['id'])
        rid=conn.execute(rels.insert().values(membership_id=row['id'],kind='staff',**evidence).returning(rels.c.id)).scalar_one()
        aid=conn.execute(assigns.insert().values(relationship_id=rid,role_id=row['approved_role_id'],**evidence).returning(assigns.c.id)).scalar_one()
        for version,action,assignment in ((1,'relationship_imported',None),(2,'role_imported',aid)):
            conn.execute(events.insert().values(membership_id=row['id'],relationship_id=rid,assignment_id=assignment,action=action,actor_user_id=None,recorded_at=now,version=version,reason='Controlled legacy import; source membership '+str(row['id'])))
        imported+=1
    return imported


def verify(conn):
    waiting.legacy.identity(conn)
    metadata=waiting.definitions()
    for name in TABLES:
        waiting.verify_table(conn,metadata.tables[name])
        indexes=sa.inspect(conn).get_indexes(name,schema='public')
        for index in indexes:
            if index['name'] in ('uq_retirement_relationship_current','uq_retirement_staff_assignment_current'):
                assert index['dialect_options']['postgresql_where'].replace('(','').replace(')','').strip()=='withdrawn_at IS NULL'
        triggers=conn.execute(sa.text("SELECT tgname,tgtype,tgenabled FROM pg_trigger WHERE tgrelid=to_regclass(:table) AND NOT tgisinternal ORDER BY tgname"),{'table':'public.'+name}).all()
        assert triggers==[(name+'_guard',31,'O'),(name+'_no_truncate',34,'O')]
    body=conn.execute(sa.text("SELECT prosrc FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND proname=:name"),{'name':FUNCTION}).scalar_one()
    assert body.strip()==BODY.strip()
    return {n:conn.execute(sa.text('SELECT count(*) FROM public.'+n)).scalar_one() for n in TABLES}


def transition():
    engine=waiting.legacy.local_engine()
    with engine.begin() as conn:
        waiting.legacy.identity(conn)
        waiting.verify(conn)
        print('Verified LOCAL ait_local_db. Inspecting legacy authority before cutover.',flush=True)
        conn.execute(sa.text('LOCK TABLE public.retirement_membership,public.retirement_organisation,public.retirement_role IN SHARE ROW EXCLUSIVE MODE'))
        candidates=inventory(conn)
        print('Evidenced legacy operational memberships:',len(candidates),flush=True)
        present=[sa.inspect(conn).has_table(n,schema='public') for n in TABLES]
        before=waiting.snapshot(conn)
        revision=conn.execute(sa.text('SELECT version_num FROM public.alembic_version')).scalars().all()
        if any(present):
            assert all(present),'Partial Stage 3 installation'
            verify(conn)
        else:
            names=list(TABLES)+[n+'_id_seq' for n in TABLES]
            assert not conn.execute(sa.text("SELECT 1 FROM pg_class WHERE relnamespace='public'::regnamespace AND relname=ANY(:names)"),{'names':names}).first(),'Stage 3 naming collision'
            assert not conn.execute(sa.text("SELECT 1 FROM pg_proc WHERE pronamespace='public'::regnamespace AND proname=:n"),{'n':FUNCTION}).first(),'Stage 3 function collision'
            metadata=waiting.definitions()
            for name in TABLES:
                conn.execute(CreateTable(metadata.tables[name]))
                for index in metadata.tables[name].indexes:conn.execute(CreateIndex(index))
            conn.execute(sa.text('CREATE FUNCTION public.'+FUNCTION+'() RETURNS trigger LANGUAGE plpgsql AS $$'+BODY+'$$'))
            for name in TABLES:
                conn.execute(sa.text('CREATE TRIGGER '+name+'_guard BEFORE INSERT OR UPDATE OR DELETE ON public.'+name+' FOR EACH ROW EXECUTE FUNCTION public.'+FUNCTION+'()'))
                conn.execute(sa.text('CREATE TRIGGER '+name+'_no_truncate BEFORE TRUNCATE ON public.'+name+' FOR EACH STATEMENT EXECUTE FUNCTION public.'+FUNCTION+'()'))
        imported=import_legacy(conn)
        after=waiting.snapshot(conn)
        for name in before:
            assert len(before[name])==len(after[name])
            for old,new in zip(before[name],after[name]):
                for k,v in old.items():
                    if name=='retirement_membership' and k.startswith('association_approved_') and v is None:continue
                    assert new[k]==v,'Legacy data changed'
        assert revision==conn.execute(sa.text('SELECT version_num FROM public.alembic_version')).scalars().all()
        counts=verify(conn)
    print('LOCAL transition verified. Imported:',imported,'Counts:',counts,'Already installed:',all(present),flush=True)
    engine.dispose()


if __name__=='__main__':
    try:
        if sys.argv[1:]==['apply']:transition()
        elif sys.argv[1:]==['verify']:
            engine=waiting.legacy.local_engine()
            with engine.connect() as conn:
                conn.exec_driver_sql('SET TRANSACTION READ ONLY');print(verify(conn))
        else:raise ValueError('Specify apply or verify')
    except Exception as error:
        print('STOPPED; transaction rolled back:',str(error) if isinstance(error,(ValueError,AssertionError)) else type(error).__name__)
        raise SystemExit(1)
