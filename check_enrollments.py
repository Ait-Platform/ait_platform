import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app import create_app
from app.extensions import db
from app.models.auth import UserEnrollment, AuthSubject

app = create_app()
with app.app_context():
    subj = AuthSubject.query.filter_by(slug='debtors').first()
    enrollments = UserEnrollment.query.filter_by(subject_id=subj.id).all()
    for e in enrollments:
        print(f"User {e.user_id}, Status: {e.status}, Country: {e.country_code}")
