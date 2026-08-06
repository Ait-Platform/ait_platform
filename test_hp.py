from app import create_app
from app.extensions import db
from sqlalchemy import text
from app.utils.queries import BRIDGE_QUERY

app = create_app()
with app.app_context():
    users = db.session.execute(text("SELECT id, email FROM \"user\" WHERE email LIKE '%hp%'")).fetchall()
    print('Users with hp:', users)
    
    # Let's check enrollments for all users to see if ANY user has an enrollment for staff
    staff = db.session.execute(text("SELECT id FROM auth_subject WHERE slug='staff'")).scalar()
    enrs = db.session.execute(text("SELECT user_id FROM user_enrollment WHERE subject_id=:sid"), {"sid": staff}).fetchall()
    print("Users enrolled in staff:", enrs)
