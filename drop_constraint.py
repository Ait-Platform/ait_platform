from app import create_app, db
from sqlalchemy import text

app = create_app()

with app.app_context():
    try:
        db.session.execute(text("ALTER TABLE bil_meter DROP CONSTRAINT bil_meter_meter_number_key;"))
        db.session.commit()
        print("Constraint dropped successfully.")
    except Exception as e:
        print(f"Error dropping constraint: {e}")
        db.session.rollback()
