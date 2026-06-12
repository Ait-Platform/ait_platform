import os
from flask import current_app
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()

with app.app_context():
    db.session.execute(text("UPDATE auth_subject SET commercial_mode = 'free' WHERE slug = 'loss'"))
    db.session.commit()
    print("Updated commercial_mode to 'free' for loss subject.")
