from app import create_app, db
from app.models.auth import User
from app.models.core import CoreRoleAssignment, CoreRole
from app.models.uip_governance import UipCommitteeMember

app = create_app()
with app.app_context():
    u = User.query.get(613)
    if not u:
        print("User 613 not found")
    else:
        print(f"User: {u.email}")
        roles = CoreRoleAssignment.query.filter_by(user_id=613).all()
        for r in roles:
            role = CoreRole.query.get(r.role_id)
            print(f"Role: {role.slug}")
            
        mem = UipCommitteeMember.query.filter_by(user_id=613).first()
        if mem:
            print(f"Committee Member Pos: {mem.position}")
        else:
            print("No Committee Member record")
