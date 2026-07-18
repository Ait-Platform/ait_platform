from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text("ALTER TABLE bil_muni_account ADD COLUMN valuation FLOAT DEFAULT 0.0"))
        db.session.execute(text("ALTER TABLE bil_muni_account ADD COLUMN rates_amount FLOAT DEFAULT 0.0"))
        db.session.execute(text("ALTER TABLE bil_muni_account ADD COLUMN arrangement_amount FLOAT DEFAULT 0.0"))
        db.session.execute(text("ALTER TABLE bil_muni_account ADD COLUMN arrangement_duration INTEGER DEFAULT 0"))
        db.session.commit()
        print('Columns added to bil_muni_account successfully.')
    except Exception as e:
        print('Error:', e)
