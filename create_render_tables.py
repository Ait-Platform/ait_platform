import os
from app import create_app
from app.extensions import db

os.environ['SQLALCHEMY_DATABASE_URI'] = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

app = create_app()

with app.app_context():
    db.create_all()
    print("Successfully created any missing tables on Render Postgres.")
