import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def check_db():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Checking data types of primary keys:")
        query = text("""
        SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod)
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
        WHERE con.contype = 'p' AND c.relnamespace = 'public'::regnamespace
        ORDER BY format_type(a.atttypid, a.atttypmod);
        """)
        cols = conn.execute(query).fetchall()
        for c in cols:
            print(f"PK Col: {c}")

if __name__ == "__main__":
    check_db()
