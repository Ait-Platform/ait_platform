import os
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    row = db.session.execute(text("SELECT trial_days FROM auth_subject WHERE slug = 'budget'")).fetchone()
    print(f"trial_days for budget: {row[0] if row else 'Not found'}")
