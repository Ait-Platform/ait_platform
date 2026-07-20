import os
import sys

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from sqlalchemy import create_engine, text

# Render PostgreSQL URL
PG_URL = (
    "postgresql+psycopg2://"
    "ait_platform_db_user:"
    "b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj"
    "@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432"
    "/ait_platform_db"
)

engine = create_engine(PG_URL)

try:
    with engine.connect() as conn:
        print("Checking if bank_detail_id exists in production DB...")
        try:
            conn.execute(text("SELECT bank_detail_id FROM bil_property LIMIT 1"))
            print("bank_detail_id already exists in production DB")
        except Exception:
            print("Column doesn't exist, adding it to production DB...")
            conn.execute(text("COMMIT")) # Clear failed transaction state
            conn.execute(text("ALTER TABLE bil_property ADD COLUMN bank_detail_id INTEGER REFERENCES bil_bank_detail(id)"))
            conn.execute(text("COMMIT"))
            print("Added bank_detail_id successfully to production DB")
except Exception as e:
    print(f"Failed to connect or migrate: {e}")
