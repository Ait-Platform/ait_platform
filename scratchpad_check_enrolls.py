from app import create_app
from app.extensions import db
from app.models.auth import User, UserEnrollment, AuthSubject

app = create_app()
with app.app_context():
    u = User.query.filter_by(email="all@gmail.com").first()
    if not u:
        print("User not found!")
    else:
        enrs = UserEnrollment.query.filter_by(user_id=u.id).all()
        for e in enrs:
            subj = AuthSubject.query.get(e.subject_id)
            print(f"Slug: {subj.slug}, Status: {e.status}")
