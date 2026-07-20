import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from sqlalchemy import create_engine, text

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
        res = conn.execute(text("SELECT id, code, is_used, used_by_user_id, created_at FROM voucher_token WHERE code='DF14C1CC'")).fetchone()
        if res:
            print(f"Voucher {res[1]}: used={res[2]} used_by={res[3]}")
        else:
            print("Voucher not found in production DB.")
except Exception as e:
    print(f"Error: {e}")
