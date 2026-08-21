import os
from app import create_app, db

# Render DB URL (replace postgres:// with postgresql:// if needed for SQLAlchemy)
render_url = 'postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db'

app = create_app()
app.config['SQLALCHEMY_DATABASE_URI'] = render_url

print("Connecting to Render DB...")
with app.app_context():
    # This will create tables that don't exist yet (e.g. CptdRegistration, SaceRegistration)
    db.create_all()
    print("Successfully created any missing tables in the Render DB!")
