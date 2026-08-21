import os
from app import create_app, db

app = create_app()

print("Updating Local DB...")
with app.app_context():
    db.create_all()
    print("Local DB updated.")

render_url = 'postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db'
app.config['SQLALCHEMY_DATABASE_URI'] = render_url

print("Updating Render DB...")
with app.app_context():
    db.create_all()
    print("Render DB updated.")
