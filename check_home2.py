from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    res = db.session.execute(text("SELECT id, slug, is_active FROM auth_subject WHERE slug='home2'")).fetchall()
    print(res)
