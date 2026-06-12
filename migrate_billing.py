from app.extensions import db
from app import create_app

app = create_app()

with app.app_context():
    try:
        db.session.execute(db.text('ALTER TABLE bil_property ADD COLUMN IF NOT EXISTS municipal_bill_number VARCHAR(100);'))
        db.session.execute(db.text('ALTER TABLE bil_meter ADD COLUMN IF NOT EXISTS parent_meter_id INTEGER REFERENCES bil_meter(id);'))
        db.session.commit()
        print('Columns added successfully!')
    except Exception as e:
        print(f"Error: {e}")
        db.session.rollback()
