import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def check_db():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Checking columns of auth_subject:")
        query = text("""
        SELECT attnum, attname, format_type(atttypid, atttypmod)
        FROM pg_attribute
        WHERE attrelid = 'auth_subject'::regclass AND attnum > 0
        ORDER BY attnum;
        """)
        cols = conn.execute(query).fetchall()
        for c in cols:
            print(f"Col: {c}")

        print("Checking primary key constraints specifically for auth_subject:")
        query2 = text("""
        SELECT c.conname, c.contype, a.attname
        FROM pg_constraint c
        JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
        WHERE c.conrelid = 'auth_subject'::regclass AND c.contype IN ('p', 'u');
        """)
        pks = conn.execute(query2).fetchall()
        for pk in pks:
            print(f"PK/UQ: {pk}")

if __name__ == "__main__":
    check_db()
