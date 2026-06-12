from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    res = db.session.execute(text("SELECT u.email, ue.zar_amount_cents FROM user_enrollment ue JOIN \"user\" u ON ue.user_id = u.id WHERE u.email LIKE '%loss%'")).fetchall()
    print("Enrollments for loss:", res)
