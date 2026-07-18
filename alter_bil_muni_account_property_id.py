from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text("ALTER TABLE bil_muni_account ADD COLUMN property_id INTEGER REFERENCES bil_property(id) ON DELETE CASCADE"))
        db.session.commit()
        print('property_id added to bil_muni_account successfully.')
    except Exception as e:
        print('Error adding property_id:', e)
