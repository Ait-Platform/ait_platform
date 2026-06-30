from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    queries = [
        "ALTER TABLE bil_property ADD COLUMN onboarding_status VARCHAR(50) DEFAULT 'completed'",
        "ALTER TABLE bil_property ADD COLUMN expected_bills INTEGER DEFAULT 1",
        "ALTER TABLE bil_property ADD COLUMN expected_tenants INTEGER DEFAULT 1",
        "ALTER TABLE bil_property ADD COLUMN is_bulk_metered INTEGER DEFAULT 0",
        "ALTER TABLE bil_property ADD COLUMN expected_sub_meters INTEGER DEFAULT 0"
    ]
    for q in queries:
        try:
            db.session.execute(text(q))
            db.session.commit()
            print(f"Success: {q}")
        except Exception as e:
            db.session.rollback()
            print(f"Failed (might already exist): {e}")
