from app.extensions import db
from app import create_app

app = create_app()

with app.app_context():
    try:
        db.session.execute(db.text('ALTER TABLE bil_property ADD COLUMN IF NOT EXISTS metro_arrangement_amount FLOAT DEFAULT 0.0;'))
        db.session.execute(db.text('ALTER TABLE bil_property ADD COLUMN IF NOT EXISTS metro_arrangement_duration INTEGER DEFAULT 0;'))
        db.session.execute(db.text('ALTER TABLE bil_property ADD COLUMN IF NOT EXISTS metro_rates_amount FLOAT DEFAULT 0.0;'))
        
        db.session.execute(db.text('ALTER TABLE bil_lease ADD COLUMN IF NOT EXISTS tenant_arrangement_charge FLOAT DEFAULT 0.0;'))
        db.session.execute(db.text('ALTER TABLE bil_lease ADD COLUMN IF NOT EXISTS tenant_rates_charge FLOAT DEFAULT 0.0;'))
        
        db.session.commit()
        print('Columns added successfully!')
    except Exception as e:
        print(f"Error: {e}")
        db.session.rollback()
