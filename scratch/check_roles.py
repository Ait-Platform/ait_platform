from app import create_app
from app.extensions import db
from app.models.auth import User, UserRole, Role

app = create_app()
with app.app_context():
    users = User.query.all()
    for u in users:
        roles = [r.role.slug for r in u.user_roles if r.role]
        print(f"User: {u.email}, Roles: {roles}")
