from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
app.app_context().push()

sql = "SELECT id, user_id, subject_id, status FROM user_enrollment"
r = db.session.execute(text(sql)).fetchall()
print("all user_enrollments:", r)

sql = "SELECT id, email FROM \"user\" WHERE email LIKE '%home2%'"
r = db.session.execute(text(sql)).fetchall()
print("all home2 users:", r)
