from app import create_app, db
from app.models.core import CoreOrganization, CoreInteraction
from app.models.auth import User, UserRole
from app.models.uip_governance import UipOrganogramSeat, UipCommitteeMember
from app.models.uip import UipResolution

app = create_app()
with app.app_context():
    org = CoreOrganization.query.filter(CoreOrganization.name.ilike('%Manor%')).first()
    print('Organization:', org.name, org.id)
    
    print('\n--- 1 & 2. Pending MO Access Requests (CoreInteraction) ---')
    claims = CoreInteraction.query.filter_by(organization_id=org.id, interaction_type='mo_claim').all()
    for c in claims:
        u = User.query.get(c.user_id) if c.user_id else None
        u_email = u.email if u else 'NoUser'
        print(f'Claim ID: {c.id}, Status: {c.status}, User: {c.user_name} ({u_email}), Desc: {c.description}')
        
    print('\n--- 3. Municipal Officer Role Assignments (UserRole) ---')
    roles = UserRole.query.filter_by(role_name='municipal_officer').all()
    for r in roles:
        u = User.query.get(r.user_id)
        if u:
            print(f'User ID: {u.id}, Email: {u.email}')
        
    print('\n--- 4 & 5. CURRENT MO Committee/Governance Appointments (UipCommitteeMember) ---')
    seats = UipOrganogramSeat.query.filter(UipOrganogramSeat.organization_id==org.id, UipOrganogramSeat.title.ilike('%Municipal%')).all()
    if not seats:
        print("No seats found with 'Municipal' in title.")
    for s in seats:
        apps = UipCommitteeMember.query.filter_by(seat_id=s.id).all()
        for a in apps:
            u = User.query.get(a.user_id) if a.user_id else None
            u_email = u.email if u else 'NoUser'
            res = UipResolution.query.get(a.resolution_id) if a.resolution_id else None
            res_ref = res.reference if res else 'None'
            print(f'Seat: {s.title}, Appt ID: {a.id}, User: {u_email}, Active: {a.is_active}, Start: {a.start_date}, End: {a.end_date}, Res: {res_ref}')
            
    print('\n--- 6. Checking for new user uipmo@gmail.com ---')
    u = User.query.filter_by(email='uipmo@gmail.com').first()
    if u:
        print(f'User uipmo@gmail.com found (ID={u.id}). Has role municipal_officer? {UserRole.query.filter_by(user_id=u.id, role_name="municipal_officer").count() > 0}')
    else:
        print('User uipmo@gmail.com NOT found in database.')
