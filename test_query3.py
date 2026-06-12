from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
app.app_context().push()

sql = """
SELECT u.email FROM user_enrollment ue JOIN "user" u ON u.id = ue.user_id WHERE ue.subject_id=4 LIMIT 5
"""
r = db.session.execute(text(sql)).fetchall()
print(r)
