from app import create_app, db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text('ALTER TABLE mech_job_cards ADD COLUMN next_service_due VARCHAR(100);'))
        db.session.commit()
        print("Added next_service_due to mech_job_cards")
    except Exception as e:
        print(e)
