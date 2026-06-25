from app import create_app, db
from app.models.auth import UserEnrollment
app=create_app()
app.app_context().push()
enrs=UserEnrollment.query.filter_by(subject_id=4, status='active').all()
print('Found:', len(enrs))
for e in enrs:
    e.status = 'paid'
db.session.commit()
print('DONE')
