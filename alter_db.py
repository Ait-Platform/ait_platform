from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text("ALTER TABLE mech_vehicles ADD COLUMN engine_no VARCHAR(50);"))
        db.session.execute(text("ALTER TABLE mech_vehicles ADD COLUMN gvm VARCHAR(20);"))
        db.session.execute(text("ALTER TABLE mech_vehicles ADD COLUMN tare VARCHAR(20);"))
        db.session.execute(text("ALTER TABLE mech_vehicles ADD COLUMN disk_license_no VARCHAR(50);"))
        db.session.commit()
        print("Schema updated successfully.")
    except Exception as e:
        print(f"Schema update error (maybe columns already exist?): {e}")
        db.session.rollback()
