import os
import sys

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import create_app
from app.extensions import db
from app.models.payment import YocoPayment
from sqlalchemy import text

def migrate():
    app = create_app()
    with app.app_context():
        # Drop legacy stripe_payment if it exists
        try:
            db.session.execute(text("DROP TABLE IF EXISTS stripe_payment;"))
            db.session.commit()
            print("Dropped stripe_payment table (if it existed).")
        except Exception as e:
            print(f"Error dropping stripe_payment: {e}")
            db.session.rollback()

        # Create yoco_payment table
        YocoPayment.__table__.create(db.engine, checkfirst=True)
        print("Created yoco_payment table.")

if __name__ == "__main__":
    migrate()
