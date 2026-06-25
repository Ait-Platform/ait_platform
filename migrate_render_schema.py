import os
from flask_migrate import upgrade
from app import create_app

PG_URL = (
    "postgresql+psycopg2://"
    "ait_platform_db_user:"
    "b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj"
    "@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432"
    "/ait_platform_db"
)

os.environ['DATABASE_URL'] = PG_URL

app = create_app()
app.config['SQLALCHEMY_DATABASE_URI'] = PG_URL

with app.app_context():
    print("Upgrading Render Database Schema...")
    upgrade()
    print("Done!")
