from app import create_app, db
from sqlalchemy import text

app = create_app()
with app.app_context():
    try:
        result = db.session.execute(text("SELECT letterhead_url FROM sender_profile LIMIT 1"))
        print("Column exists!")
    except Exception as e:
        print("Error:", e)
