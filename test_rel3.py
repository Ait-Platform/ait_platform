from app import create_app
from app.extensions import db
from sqlalchemy import create_engine
import os

PG_URL = (
    "postgresql+psycopg2://"
    "ait_platform_db_user:"
    "b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj"
    "@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432"
    "/ait_platform_db"
)

# Overwrite environment variable so create_app uses it
os.environ['DATABASE_URL'] = PG_URL

app = create_app()

with app.app_context():
    from app.models.mechanic import MechJobCard, MechCommunication
    jc = MechJobCard.query.get(12)
    if jc:
        print(f"Job Card {jc.id} has {len(jc.communications)} communications.")
        for c in jc.communications:
            print(f" - {c.comm_type}: {c.message}")
    else:
        print("Job Card 12 not found.")
