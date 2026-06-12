from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    r = db.session.execute(text("SELECT program_type, commercial_mode FROM auth_subject WHERE slug='home_premium'")).fetchone()
    print("program_type:", r[0], "commercial_mode:", r[1])
