from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    db.session.execute(text("INSERT INTO auth_pricing (subject_id, role, plan, currency, amount_cents, is_active, active_from) VALUES (18, 'user', 'enrollment', 'ZAR', 25000, 1, CURRENT_TIMESTAMP)"))
    db.session.commit()
    print("Pricing added successfully for home_premium!")
