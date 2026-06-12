import os
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    res = db.session.execute(text('SELECT u.email, s.slug, e.status FROM user_enrollment e JOIN "user" u ON u.id=e.user_id JOIN auth_subject s ON s.id=e.subject_id')).fetchall()
    print("ENROLLMENTS:")
    for r in res:
        print(r)
