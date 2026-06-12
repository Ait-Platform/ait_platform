import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def check_db():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        query = text("""
        SELECT conname, contype, conkey
        FROM pg_constraint
        WHERE conrelid = 'auth_subject'::regclass;
        """)
        result = conn.execute(query).fetchall()
        for r in result:
            print(f"Constraint: {r}")

if __name__ == "__main__":
    check_db()
