import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def check_db():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Checking all BIGINT columns:")
        query = text("""
        SELECT c.relname, a.attname
        FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE a.atttypid = 'bigint'::regtype
        AND n.nspname = 'public' AND c.relkind = 'r';
        """)
        cols = conn.execute(query).fetchall()
        for c in cols:
            print(f"Table: {c[0]}, Col: {c[1]}")

if __name__ == "__main__":
    check_db()
