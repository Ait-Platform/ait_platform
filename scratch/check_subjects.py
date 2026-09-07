from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    rows = db.session.execute(text("SELECT id, slug, name, commercial_mode FROM auth_subject ORDER BY id")).fetchall()
    for r in rows:
        print(f"ID: {r.id}, Slug: {r.slug}, Name: {r.name}, Mode: {r.commercial_mode}")
