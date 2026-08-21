from app import create_app
from app.extensions import db
from sqlalchemy import create_engine, text

app = create_app()
PG_URL = (
    "postgresql+psycopg2://"
    "ait_platform_db_user:"
    "b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj"
    "@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432"
    "/ait_platform_db"
)
app.config['SQLALCHEMY_DATABASE_URI'] = PG_URL
with app.app_context():
    from app.models.mechanic import MechJobCard, MechCommunication
    jc = MechJobCard.query.order_by(MechJobCard.id.desc()).first()
    if jc:
        print(f"Latest Job Card ID: {jc.id}")
        print(f"Has {len(jc.communications)} communications.")
        for c in jc.communications:
            print(f" - {c.comm_type}: {c.message}")
    else:
        print("No job cards found.")
