import os
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    subjects = db.session.execute(text("SELECT id, slug FROM auth_subject")).mappings().all()
    for s in subjects:
        print(f"ID: {s['id']}, Slug: {s['slug']}")
