from app import create_app
from app.extensions import db
from app.models.uip import UipMemberProfile

app = create_app()
with app.app_context():
    rp = UipMemberProfile.query.filter_by(email="ayesha.khan@example.invalid").first()
    if rp:
        print("Found:", rp.name, rp.email, rp.is_active)
    else:
        print("Not found")
