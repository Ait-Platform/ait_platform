from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    db.session.execute(text("UPDATE auth_subject SET enroll_policy = 'manual' WHERE id IN (22, 23, 24)"))
    db.session.execute(text("DELETE FROM user_enrollment WHERE user_id = 515 AND subject_id IN (22, 23, 24)"))
    db.session.commit()
    print("Done")
