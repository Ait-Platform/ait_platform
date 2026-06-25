from wsgi import app
from app.extensions import db
from sqlalchemy import text

with app.app_context():
    user_id = db.session.execute(text("SELECT id FROM \"user\" WHERE email='spv@gmail.com'")).scalar()
    subject_id = db.session.execute(text("SELECT id FROM auth_subject WHERE slug='spv'")).scalar()
    
    if user_id and subject_id:
        existing = db.session.execute(text("SELECT id FROM user_enrollment WHERE user_id=:uid AND subject_id=:sid"), {"uid": user_id, "sid": subject_id}).scalar()
        if not existing:
            db.session.execute(text("INSERT INTO user_enrollment (user_id, subject_id, status) VALUES (:uid, :sid, 'active')"), {"uid": user_id, "sid": subject_id})
            db.session.commit()
            print("Successfully enrolled spv@gmail.com into 'spv' program!")
        else:
            print("User is already enrolled.")
    else:
        print("User or subject not found.")
