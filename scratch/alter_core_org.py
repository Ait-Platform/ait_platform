from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text("ALTER TABLE core_organization ADD COLUMN area VARCHAR(255);"))
        db.session.execute(text("ALTER TABLE core_organization ADD COLUMN municipality_ref VARCHAR(255);"))
        db.session.execute(text("ALTER TABLE core_organization ADD COLUMN contact_email VARCHAR(255);"))
        db.session.execute(text("ALTER TABLE core_organization ADD COLUMN contact_phone VARCHAR(50);"))
        db.session.execute(text("ALTER TABLE core_organization ADD COLUMN status VARCHAR(50) DEFAULT 'active';"))
        db.session.execute(text("ALTER TABLE core_organization ADD COLUMN config_json TEXT;"))
        db.session.commit()
        print("Columns added successfully.")
    except Exception as e:
        print(f"Error adding columns (they might already exist): {e}")
        db.session.rollback()
