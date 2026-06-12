from app import create_app, db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        db.session.execute(text("ALTER TABLE bil_tenant ADD COLUMN email_statements BOOLEAN DEFAULT FALSE;"))
        db.session.commit()
        print("Column added successfully.")
    except Exception as e:
        print(f"Error: {e}")
