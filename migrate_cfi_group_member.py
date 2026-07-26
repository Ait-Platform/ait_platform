from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()

with app.app_context():
    try:
        db.session.execute(text("ALTER TABLE cfi_group_members ADD COLUMN status VARCHAR(20) DEFAULT 'accepted';"))
        db.session.commit()
        print("Successfully added 'status' column to 'cfi_group_members'.")
    except Exception as e:
        db.session.rollback()
        print(f"Error (column might already exist): {e}")
