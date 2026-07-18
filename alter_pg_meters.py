from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    with db.engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE bil_property ADD COLUMN expected_water_meters INTEGER DEFAULT 0;"))
            conn.commit()
            print("Added expected_water_meters to PostgreSQL")
        except Exception as e:
            conn.rollback()
            print(f"expected_water_meters already exists? {e}")
            
        try:
            conn.execute(text("ALTER TABLE bil_property ADD COLUMN expected_elec_meters INTEGER DEFAULT 0;"))
            conn.commit()
            print("Added expected_elec_meters to PostgreSQL")
        except Exception as e:
            conn.rollback()
            print(f"expected_elec_meters already exists? {e}")
