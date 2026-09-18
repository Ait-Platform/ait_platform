from app import create_app, db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text("ALTER TABLE uip_member_profile ADD COLUMN invite_wave INTEGER DEFAULT 0;"))
        db.session.execute(text("ALTER TABLE uip_member_profile ADD COLUMN last_invite_at TIMESTAMP;"))
        db.session.commit()
        print("Successfully added tracking columns to uip_member_profile")
    except Exception as e:
        db.session.rollback()
        print(f"Columns might already exist or error: {e}")
