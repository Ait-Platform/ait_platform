import os
from app import create_app
from app.extensions import db
from app.models.auth import User
from app.models.uip_governance import UipCommitteeMember

app = create_app()
with app.app_context():
    for uid in [614, 624]:
        u = User.query.get(uid)
        print(f"\nUser {uid}: {u.email if u else 'Not found'} (Active: {u.is_active if u else 'N/A'})")
        if u:
            cms = UipCommitteeMember.query.filter(
                UipCommitteeMember.organization_id == 1
            ).all()
            found = False
            for cm in cms:
                if cm.user_id == uid or (cm.email and u.email and cm.email.strip().lower() == u.email.strip().lower()):
                    print(f"  -> Found in Committee! Position: {cm.position}, Status: {cm.status}, user_id: {cm.user_id}, DB email: '{cm.email}'")
                    found = True
            if not found:
                print("  -> NOT found in Committee by user_id or exact email match!")
