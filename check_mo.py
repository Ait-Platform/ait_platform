from app import create_app, db
from app.models.core import CoreOrganization
from app.models.uip import UipAccessRequest, UipCommitteeMember, UipCommitteeSeat, UipResolution
from app.models.auth import AuthUser, AuthUserRole

app = create_app()
with app.app_context():
    org = CoreOrganization.query.filter(CoreOrganization.name.ilike('%Manor%')).first()
    print('ORG ID:', org.id)
    
    reqs = UipAccessRequest.query.filter_by(organization_id=org.id, role_requested='municipal_officer').all()
    print('\nMO Requests:')
    for r in reqs:
        u = AuthUser.query.get(r.user_id) if r.user_id else None
        u_email = u.email if u else 'NoUser'
        print(f'Req: ID={r.id}, UserID={r.user_id} ({u_email}), Status={r.status}, Email={r.applicant_email}')
        
    roles = AuthUserRole.query.filter_by(role_name='municipal_officer').all()
    print('\nMO Roles:')
    for role in roles:
        u = AuthUser.query.get(role.user_id)
        print(f'Role: UserID={u.id}, Email={u.email}')
        
    seats = UipCommitteeSeat.query.filter(UipCommitteeSeat.organization_id==org.id, UipCommitteeSeat.seat_name=='Municipal Officer').all()
    print('\nMO Seats/Appointments:')
    for s in seats:
        apps = UipCommitteeMember.query.filter_by(seat_id=s.id).all()
        for a in apps:
            res = UipResolution.query.get(a.resolution_id) if a.resolution_id else None
            res_ref = res.reference if res else 'None'
            u = AuthUser.query.get(a.user_id)
            print(f'Appt: UserID={u.id} {u.email}, Active={a.is_active}, Start={a.start_date}, End={a.end_date}, Res={res_ref}')
