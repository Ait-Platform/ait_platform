from app import create_app
from app.extensions import db
from app.models.auth import UserEnrollment, AuthSubject
from datetime import datetime, timedelta

app = create_app()
with app.app_context():
    subj = AuthSubject.query.filter_by(slug='budget').first()
    if subj and subj.trial_days:
        enrollments = UserEnrollment.query.filter_by(subject_id=subj.id, status='pending').all()
        for ue in enrollments:
            ue.status = 'active'
            ue.trial_end = datetime.utcnow() + timedelta(days=float(subj.trial_days))
        db.session.commit()
        print('Updated', len(enrollments), 'pending budget enrollments to active.')
