from app.extensions import db
from app import create_app

app = create_app()
with app.app_context():
    db.session.execute(db.text("DELETE FROM user_enrollment WHERE user_id IN (SELECT id FROM \"user\" WHERE email = 'home@gmail.com') AND subject_id != 4"))
    db.session.commit()
    print("Cleaned up enrollments for home@gmail.com.")
