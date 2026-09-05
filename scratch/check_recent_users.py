from app import create_app
from app.extensions import db
from app.models.auth import User

app = create_app()
with app.app_context():
    # Find recently created users to see the SACE member
    users = User.query.order_by(User.id.desc()).limit(10).all()
    for u in users:
        print(f"ID: {u.id}, Email: {u.email}, Roles: {[r.role.slug for r in u.user_roles if r.role]}")
