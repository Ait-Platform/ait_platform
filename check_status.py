from app import create_app
from app.extensions import db

app = create_app()
with app.app_context():
    res = db.session.execute(db.text("SELECT u.email, ue.status, ue.subject_id FROM user_enrollment ue JOIN \"user\" u ON ue.user_id = u.id WHERE u.email = 'read2@gmail.com'")).fetchall()
    print("USER STATUS:", res)
