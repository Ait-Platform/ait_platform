import os
import sys

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()

with app.app_context():
    try:
        # Check if it exists
        res = db.session.execute(text("SELECT bank_detail_id FROM bil_property LIMIT 1"))
        print("bank_detail_id already exists")
    except Exception as e:
        db.session.rollback()
        print("Column doesn't exist, adding it...")
        # Add column
        db.session.execute(text("ALTER TABLE bil_property ADD COLUMN bank_detail_id INTEGER REFERENCES bil_bank_detail(id)"))
        db.session.commit()
        print("Added bank_detail_id successfully")
