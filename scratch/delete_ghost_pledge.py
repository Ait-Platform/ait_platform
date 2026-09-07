from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    db.session.execute(text("DELETE FROM sace_workshop_interaction WHERE user_id = 1 AND activity_slug = 'admin_patent_pledge'"))
    db.session.commit()
    print("Deleted ghost pledges.")
