from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()

with app.app_context():
    try:
        db.session.execute(text("ALTER TABLE crm_enquiry ADD COLUMN medical_aid_plan VARCHAR(150);"))
        db.session.commit()
        print("Successfully added medical_aid_plan column to crm_enquiry table.")
    except Exception as e:
        print(f"Error: {e}")
        db.session.rollback()
