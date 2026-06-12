from app import create_app
from app.extensions import db

app = create_app()
with app.app_context():
    db.session.execute(db.text("DELETE FROM user_enrollment WHERE subject_id IN (SELECT id FROM auth_subject WHERE slug = 'cultural_fire')"))
    db.session.commit()
    print('Deleted cfi enrollments')
