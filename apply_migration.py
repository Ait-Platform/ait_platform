from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text("ALTER TABLE uip_organogram_seat ADD COLUMN duty VARCHAR(50) DEFAULT 'committee_member';"))
        db.session.commit()
        print("Successfully added duty column")
    except Exception as e:
        print("Error:", e)
