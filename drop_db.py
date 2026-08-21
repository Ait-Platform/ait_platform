from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text("ALTER TABLE mech_vehicles DROP COLUMN engine_no;"))
        db.session.execute(text("ALTER TABLE mech_vehicles DROP COLUMN gvm;"))
        db.session.execute(text("ALTER TABLE mech_vehicles DROP COLUMN tare;"))
        db.session.execute(text("ALTER TABLE mech_vehicles DROP COLUMN disk_license_no;"))
        db.session.commit()
        print("Schema dropped locally.")
    except Exception as e:
        print(f"Error dropping columns: {e}")
        db.session.rollback()
