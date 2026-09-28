from app import create_app, db
from app.models.uip_governance import UipCommitteeMember
from app.models.auth import User, UserRole
from app.models.core import CoreOrganization

app = create_app()
with app.app_context():
    org = CoreOrganization.query.filter(CoreOrganization.name.ilike('%Manor%')).first()
    
    # 1. Look for MOs in UipCommitteeMember
    mos = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org.id,
        UipCommitteeMember.position.ilike('%Municipal%')
    ).all()
    print("--- MO Appointments in Committee ---")
    for mo in mos:
        u = User.query.get(mo.user_id) if mo.user_id else None
        print(f"Name: {mo.name}, Email: {mo.email}, Status: {mo.status}, User_ID: {mo.user_id}, User_Email: {u.email if u else 'None'}")
        
    # 2. Look for uipmo@gmail.com
    print("\n--- Check uipmo@gmail.com ---")
    u = User.query.filter_by(email='uipmo@gmail.com').first()
    if u:
        print(f"Found: {u.email} (ID {u.id})")
        roles = UserRole.query.filter_by(user_id=u.id).all()
        print("Roles:", [r.role_id for r in roles])
    else:
        print("uipmo@gmail.com not found in Users")
