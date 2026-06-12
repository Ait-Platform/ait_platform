from app.extensions import db
from app import create_app

app = create_app()

with app.app_context():
    try:
        db.session.execute(db.text('ALTER TABLE bil_lease ADD COLUMN IF NOT EXISTS tenant_arrears_total FLOAT DEFAULT 0.0;'))
        db.session.execute(db.text('ALTER TABLE bil_lease ADD COLUMN IF NOT EXISTS tenant_arrears_installment FLOAT DEFAULT 0.0;'))
        db.session.execute(db.text('ALTER TABLE bil_lease ADD COLUMN IF NOT EXISTS agent_fee_amount FLOAT DEFAULT 0.0;'))
        db.session.execute(db.text("ALTER TABLE bil_lease ADD COLUMN IF NOT EXISTS agent_fee_target VARCHAR(50) DEFAULT 'owner';"))
        
        db.session.commit()
        print('Columns added successfully!')
    except Exception as e:
        print(f"Error: {e}")
        db.session.rollback()
