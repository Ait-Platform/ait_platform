from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text("ALTER TABLE bil_meter ADD COLUMN status VARCHAR(50) DEFAULT 'active'"))
        db.session.commit()
        print('Status column added to bil_meter successfully.')
    except Exception as e:
        print('Error:', e)
