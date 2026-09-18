from app import create_app, db
from app.models.auth import User
from app.models.core import CoreRoleAssignment, CoreRole

app = create_app()
with app.app_context():
    u = User.query.get(613)
    if u:
        print(f"User 613 exists: {u.email}")
        roles = CoreRoleAssignment.query.filter_by(user_id=613).all()
        if roles:
            for r in roles:
                role = CoreRole.query.get(r.role_id)
                print(f"Has role: {role.slug}")
        else:
            print("User 613 has NO ROLES!")
    else:
        print("User 613 DOES NOT EXIST in the database!")
