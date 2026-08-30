from app import create_app, db
from app.models.auth import User
from app.models.core import CoreOrganization, CoreRole, CoreOrganizationMember, CoreRoleAssignment, CoreOrganizationWallet

app = create_app()
app.app_context().push()

print("Bootstrapping Manor Gardens UIP...")

# 1. Create Organization
org = CoreOrganization.query.filter_by(slug="manor-gardens").first()
if not org:
    org = CoreOrganization(name="Manor Gardens UIP", slug="manor-gardens")
    db.session.add(org)
    db.session.commit()
    print("Created org: Manor Gardens UIP")

# 2. Create Wallet
wallet = CoreOrganizationWallet.query.filter_by(organization_id=org.id).first()
if not wallet:
    wallet = CoreOrganizationWallet(organization_id=org.id, balance=1000)
    db.session.add(wallet)
    db.session.commit()
    print("Provisioned Wallet with 1,000 AIT Tokens.")

# 3. Ensure Roles Exist
roles = ["manager", "receptionist", "resident", "committee_member"]
role_objs = {}
for r in roles:
    role = CoreRole.query.filter_by(slug=r).first()
    if not role:
        role = CoreRole(name=r.replace('_', ' ').title(), slug=r)
        db.session.add(role)
    role_objs[r] = role
db.session.commit()

# 4. Create Users and Assign Roles
demo_users = [
    {"email": "manager@manorgardens.co.za", "role": "manager", "name": "Alice Manager"},
    {"email": "reception@manorgardens.co.za", "role": "receptionist", "name": "Bob Receptionist"},
    {"email": "resident1@manorgardens.co.za", "role": "resident", "name": "Charlie Resident"},
    {"email": "committee@manorgardens.co.za", "role": "committee_member", "name": "Diana Committee"}
]

for du in demo_users:
    user = User.query.filter_by(email=du["email"]).first()
    if not user:
        user = User(email=du["email"], name=du["name"])
        user.set_password("password123")
        user.active = True
        db.session.add(user)
        db.session.commit()
        
    # Make member
    member = CoreOrganizationMember.query.filter_by(organization_id=org.id, user_id=user.id).first()
    if not member:
        member = CoreOrganizationMember(organization_id=org.id, user_id=user.id, is_active=True)
        db.session.add(member)
        
    # Assign role
    assignment = CoreRoleAssignment.query.filter_by(user_id=user.id, organization_id=org.id).first()
    if not assignment:
        assignment = CoreRoleAssignment(user_id=user.id, organization_id=org.id, role_id=role_objs[du["role"]].id)
        db.session.add(assignment)
        
db.session.commit()
print("Bootstrapped demo users and roles.")
print("Run python scripts/bootstrap_manor_gardens.py anytime to reset the pilot.")
