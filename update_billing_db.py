from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text('ALTER TABLE bil_property ADD COLUMN wallet_balance_cents INTEGER NOT NULL DEFAULT 0;'))
        db.session.execute(text('ALTER TABLE bil_property ADD COLUMN trial_ends_at TIMESTAMP;'))
        db.session.execute(text('ALTER TABLE bil_property ADD COLUMN shadow_spent_cents INTEGER NOT NULL DEFAULT 0;'))
        db.session.commit()
        print('Billing columns added successfully!')
    except Exception as e:
        print('Error:', e)
