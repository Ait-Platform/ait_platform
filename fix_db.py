from app import create_app, db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text('ALTER TABLE cfi_shows ADD COLUMN is_private BOOLEAN DEFAULT FALSE'))
        db.session.commit()
        print("Column added successfully.")
    except Exception as e:
        print("Error:", e)
        db.session.rollback()
