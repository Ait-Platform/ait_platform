from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
app.app_context().push()

try:
    db.session.execute(text("DELETE FROM user_enrollment WHERE user_id IN (SELECT id FROM \"user\" WHERE email='read1@gmail.com')"))
    db.session.execute(text("DELETE FROM user_roles WHERE user_id IN (SELECT id FROM \"user\" WHERE email='read1@gmail.com')"))
    db.session.execute(text("DELETE FROM \"user\" WHERE email='read1@gmail.com'"))
    db.session.commit()
    print('Cleaned up read1@gmail.com')
except Exception as e:
    print(e)
