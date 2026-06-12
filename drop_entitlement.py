import os
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text("DROP TABLE user_entitlement CASCADE;"))
        db.session.commit()
        print("Dropped user_entitlement successfully.")
    except Exception as e:
        db.session.rollback()
        print(f"Failed to drop user_entitlement: {e}")
