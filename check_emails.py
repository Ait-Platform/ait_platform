import sys
from app import create_app
app = create_app()
with app.app_context():
    from app.models.uip import UipMemberProfile
    members = UipMemberProfile.query.all()
    print("Found", len(members), "members")
    for m in members:
        print(f"ID:{m.id} Email:{m.email} Name:{m.name}")
