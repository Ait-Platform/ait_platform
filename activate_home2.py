from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    db.session.execute(text("UPDATE auth_subject SET is_active = 1 WHERE slug='home2'"))
    db.session.commit()
    print('home2 activated!')
