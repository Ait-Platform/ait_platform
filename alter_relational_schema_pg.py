from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text("ALTER TABLE bil_muni_account ADD COLUMN is_bulk_account BOOLEAN DEFAULT FALSE"))
        db.session.commit()
        print('is_bulk_account added.')
    except Exception as e:
        db.session.rollback()
        print('Error:', e)
        
    try:
        db.session.execute(text("ALTER TABLE bil_meter ADD COLUMN replacement_for_meter_id INTEGER REFERENCES bil_meter(id)"))
        db.session.commit()
        print('replacement_for_meter_id added.')
    except Exception as e:
        db.session.rollback()
        print('Error:', e)
