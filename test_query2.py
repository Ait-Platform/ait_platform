from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
app.app_context().push()

sql = """
        SELECT ue.status, u.email, s.slug, s.commercial_mode
        FROM user_enrollment ue
        JOIN "user" u ON u.id = ue.user_id
        JOIN auth_subject s ON s.id = ue.subject_id
        WHERE lower(u.email) = 'home2@gmail.com'
"""
r = db.session.execute(text(sql)).fetchall()
print("raw enrollments:", r)
