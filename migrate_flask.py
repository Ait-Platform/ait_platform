from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()

with app.app_context():
    try:
        db.session.execute(text('ALTER TABLE crm_enquiry ADD COLUMN patient_id_no VARCHAR(50);'))
        print("Added patient_id_no")
    except Exception as e:
        print(f"Error adding patient_id_no: {e}")
        db.session.rollback()

    try:
        db.session.execute(text('ALTER TABLE crm_enquiry ADD COLUMN medical_aid_no VARCHAR(100);'))
        print("Added medical_aid_no")
    except Exception as e:
        print(f"Error adding medical_aid_no: {e}")
        db.session.rollback()

    db.session.commit()
    print("Done.")
