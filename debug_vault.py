from app import create_app
from app.extensions import db
from app.models.uip import UipMemberProfile

app = create_app()
with app.app_context():
    org_id = 1
    profiles = UipMemberProfile.query.filter_by(organization_id=org_id).all()
    print(f"Total members in vault: {len(profiles)}")
    for p in profiles:
        print(f" - {p.name} ({p.email})")
