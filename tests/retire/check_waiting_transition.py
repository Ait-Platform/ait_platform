"""Populate legacy history, run explicit local transition twice, compare and clean."""
from pathlib import Path
import sys
import uuid
import sqlalchemy as sa
sys.path.insert(0,str(Path(__file__).resolve().parents[2] / 'scripts'))
import retirement_stage2_local as local
import retirement_waiting_room_transition as transition

engine = local.local_engine()
ids = []
try:
    with engine.begin() as conn:
        local.identity(conn)
        # Read-only preflight must precede even test fixture writes.
        if sa.inspect(conn).has_table('retirement_waiting_user',schema='public'):
            transition.verify(conn)
        else:
            module, metadata = local.definitions()
            local.verify(conn,metadata,module)
        users = conn.execute(sa.text('SELECT id FROM public."user" ORDER BY id LIMIT 2')).scalars().all()
        assert len(users)==2
        roles = dict(conn.execute(sa.text('SELECT code,id FROM public.retirement_role')).all())
        for state in ('pending','active','denied','disabled'):
            oid = conn.execute(sa.text('INSERT INTO public.retirement_organisation(name,owner_user_id) VALUES (:n,:u) RETURNING id'),{'n':'waiting_preservation_'+uuid.uuid4().hex,'u':users[0]}).scalar_one()
            ids.append(oid)
            conn.execute(sa.text("INSERT INTO public.retirement_membership(organisation_id,user_id,status,approved_role_id) VALUES (:o,:u,'active',:r)"),{'o':oid,'u':users[0],'r':roles['organisation_owner']})
            conn.execute(sa.text("INSERT INTO public.retirement_membership(organisation_id,user_id,status,requested_role_id,approved_role_id,reviewed_at,reviewed_by_user_id,review_reason) VALUES (:o,:u,:s,:r,:a,'2025-01-02T03:04:05Z',:reviewer,'Preservation fixture history')"),{'o':oid,'u':users[1],'s':state,'r':roles['care_staff'],'a':roles['care_staff'] if state in ('active','disabled') else None,'reviewer':users[0]})
        before = transition.snapshot(conn)
    transition.transition()
    transition.transition()
    with engine.connect() as conn:
        after = transition.snapshot(conn)
        for name in before:
            assert len(before[name])==len(after[name])
            for old,new in zip(before[name],after[name]):
                assert all(new[k]==v for k,v in old.items())
        print('PASS: populated transition preservation: 4 homes, 8 memberships; all IDs, four legacy statuses, roles and review history unchanged; repeat is a no-op.')
finally:
    if ids:
        with engine.begin() as conn:
            local.identity(conn)
            conn.execute(sa.text('DELETE FROM public.retirement_membership WHERE organisation_id=ANY(:ids)'),{'ids':ids})
            conn.execute(sa.text('DELETE FROM public.retirement_organisation WHERE id=ANY(:ids)'),{'ids':ids})
        print('Only preservation fixtures cleaned up.')
