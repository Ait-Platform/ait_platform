from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()

with app.app_context():
    try:
        db.session.execute(text('ALTER TABLE cfi_showcase_votes ADD COLUMN score INTEGER DEFAULT 0'))
        db.session.commit()
        print("Successfully added score column!")
    except Exception as e:
        print("Error or column already exists:", e)
        db.session.rollback()
