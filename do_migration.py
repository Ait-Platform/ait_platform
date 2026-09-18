from dotenv import load_dotenv
import os

load_dotenv()
from app import create_app, db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text("ALTER TABLE core_interaction ADD COLUMN parent_id INTEGER REFERENCES core_interaction(id);"))
        db.session.commit()
        print("SUCCESS: parent_id added to core_interaction")
    except Exception as e:
        db.session.rollback()
        print(f"FAILED: {e}")
