import os
from dotenv import load_dotenv
load_dotenv()

from app import create_app
from app.extensions import db
from app.models.auth import User
from app.models.core import CoreRoleAssignment
from app.models.uip_governance import UipCommitteeMember, UipSubcommitteeMembership

app = create_app()
with app.app_context():
    user = User.query.get(624)
    if not user:
        print("User 624 not found")
    else:
        print(f"User 624: {user.name} ({user.email})")
        
        print("\n--- System Roles (AIT Core) ---")
        roles = CoreRoleAssignment.query.filter_by(user_id=624).all()
        for r in roles:
            print(f"- {r.role.slug}")
            
        print("\n--- Committee Roles (UIP Governance) ---")
        committee = UipCommitteeMember.query.filter(
            UipCommitteeMember.status == "CURRENT",
            db.func.lower(UipCommitteeMember.email) == db.func.lower(user.email)
        ).all()
        for c in committee:
            print(f"- Position: {c.position}")
            
