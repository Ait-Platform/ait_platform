from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    db.session.execute(text("DELETE FROM user_enrollment WHERE user_id IN (SELECT id FROM \"user\" WHERE email LIKE '%loss%')"))
    db.session.commit()
    print("Cleared user enrollments for loss test accounts.")
