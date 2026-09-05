from app import create_app
from app.extensions import db

app = create_app()
with app.app_context():
    # Only creates missing tables, doesn't drop anything.
    db.create_all()
    print("Local database schema stabilized. Missing tables created.")
