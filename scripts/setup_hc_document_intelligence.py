import os
import sys

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__))))

from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()

with app.app_context():
    try:
        # Add status to HcDocument
        db.session.execute(text("ALTER TABLE hc_document ADD COLUMN status VARCHAR(50) DEFAULT 'uploaded';"))
        print("Added status to hc_document.")
    except Exception as e:
        print("Warning: status column might already exist.", e)
        db.session.rollback()

    try:
        db.session.execute(text("""
            CREATE TABLE IF NOT EXISTS hc_document_extraction (
                id SERIAL PRIMARY KEY,
                document_id INTEGER NOT NULL REFERENCES hc_document(id) ON DELETE CASCADE,
                extracted_json TEXT,
                document_type VARCHAR(50),
                reviewed BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """))
        print("Created hc_document_extraction table.")
    except Exception as e:
        print("Warning: hc_document_extraction table creation failed.", e)
        db.session.rollback()
        
    db.session.commit()
