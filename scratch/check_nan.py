from app import create_app
from app.extensions import db
from app.models.auth import User

app = create_app()
with app.app_context():
    u = User.query.filter_by(email='nan@gmail.com').first()
    if u:
        roles = [r.role.slug for r in u.user_roles if r.role]
        print(f"nan@gmail.com roles: {roles}")
        
        # Check if they are in auth_approved_admin
        from app.models.auth import ApprovedAdmin
        is_admin = ApprovedAdmin.query.filter_by(email='nan@gmail.com').first()
        print(f"Is in ApprovedAdmin: {is_admin is not None}")
        
        # Check enrollments
        from app.models.auth import UserEnrollment
        enrolls = UserEnrollment.query.filter_by(user_id=u.id).all()
        print(f"Enrollments: {[(e.subject_id, e.status) for e in enrolls]}")
    else:
        print("nan@gmail.com not found")
