from app import create_app
from app.extensions import db
from app.models.core import CoreRoleAssignment, CoreRole

app = create_app()
with app.app_context():
    assignments = CoreRoleAssignment.query.filter_by(user_id=614).all()
    print("User 614 roles:")
    for a in assignments:
        print(f"- {a.role.slug}")
