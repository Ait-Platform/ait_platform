from app import create_app
from app.extensions import db
from app.models.auth import UserEnrollment, User
from app.models.payment import SubjectCountryPrice

app = create_app()
with app.app_context():
    u = User.query.filter_by(email='sanjith@gmail.com').first()
    if u:
        enrollments = UserEnrollment.query.filter_by(user_id=u.id).all()
        for e in enrollments:
            print(f"Enrollment {e.subject.name}: {e.local_amount_cents} {e.local_currency} (ZAR {e.zar_amount_cents})")
