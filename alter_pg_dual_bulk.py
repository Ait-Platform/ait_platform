from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    with db.engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE bil_property ADD COLUMN is_bulk_water BOOLEAN DEFAULT FALSE;"))
            conn.commit()
            print("Added is_bulk_water to PostgreSQL")
        except Exception as e:
            conn.rollback()
            print(f"is_bulk_water already exists? {e}")
            
        try:
            conn.execute(text("ALTER TABLE bil_property ADD COLUMN is_bulk_elec BOOLEAN DEFAULT FALSE;"))
            conn.commit()
            print("Added is_bulk_elec to PostgreSQL")
        except Exception as e:
            conn.rollback()
            print(f"is_bulk_elec already exists? {e}")
