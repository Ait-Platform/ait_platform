from app import create_app
from app.extensions import db
from app.models.core import CoreOrganization, CoreRole, CorePermission, CoreRolePermission

app = create_app()
with app.app_context():
    # 1. Create Manor Gardens Organisation
    mg = CoreOrganization.query.filter_by(slug="manor_gardens").first()
    if not mg:
        mg = CoreOrganization(name="Manor Gardens UIP", slug="manor_gardens")
        db.session.add(mg)
        db.session.flush()
        print("Created Manor Gardens UIP Organisation.")
    
    # 2. Create Base Roles for Manor Gardens
    roles = ["Resident", "Receptionist", "Manager", "Committee Member", "Finance Officer", "Service Provider"]
    for r in roles:
        slug = r.lower().replace(" ", "_")
        role_obj = CoreRole.query.filter_by(slug=slug, organization_id=mg.id).first()
        if not role_obj:
            role_obj = CoreRole(name=r, slug=slug, organization_id=mg.id)
            db.session.add(role_obj)
            print(f"Created role: {r}")

    db.session.commit()
    print("Phase 9: Manor Gardens Configuration seeded successfully.")
