import os
from app import create_app
from app.extensions import db

app = create_app()
with app.app_context():
    # Only create new tables
    db.create_all()
    print("Tables created successfully.")
