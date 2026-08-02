import os
from app import create_app
from app.extensions import db

DATABASE_URL = "postgresql://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

app = create_app()
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL

with app.app_context():
    print("Creating all tables in Render DB...")
    db.create_all()
    print("Done!")
