import os
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        # First, ensure subject_id 4 doesn't already have pricing we're blindly duplicating
        existing = db.session.execute(
            text("SELECT COUNT(*) FROM subject_country_price WHERE subject_id = 4")
        ).scalar()
        
        if existing > 0:
            print(f"subject_id=4 already has {existing} pricing records! Consider deleting them first or manually reviewing.")
        else:
            # Copy from subject_id=1 to subject_id=4
            db.session.execute(
                text("""
                    INSERT INTO subject_country_price (subject_id, country_code, currency_code, amount_cents, is_active, display_amount)
                    SELECT 4, country_code, currency_code, amount_cents, is_active, display_amount
                    FROM subject_country_price
                    WHERE subject_id = 1
                """)
            )
            db.session.commit()
            print("Successfully copied pricing from subject_id=1 to subject_id=4!")
    except Exception as e:
        db.session.rollback()
        print(f"Error copying pricing: {e}")
