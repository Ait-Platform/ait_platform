import os
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        # First, delete any lingering zero-prices if they exist (just in case)
        db.session.execute(text("DELETE FROM subject_country_price WHERE subject_id = 2"))
        
        # Copy from subject_id=1 to subject_id=2 (Billing)
        db.session.execute(
            text("""
                INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active, price_version)
                SELECT 2, country_code, local_amount_cents, zar_amount_cents, local_currency, is_active, price_version
                FROM subject_country_price
                WHERE subject_id = 1
            """)
        )
        db.session.commit()
        print("Successfully copied pricing from subject_id=1 (Reading) to subject_id=2 (Billing)!")
    except Exception as e:
        db.session.rollback()
        print(f"Error copying pricing: {e}")
