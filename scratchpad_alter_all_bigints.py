import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def check_db():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Finding BIGINT primary keys...")
        query = text("""
        SELECT c.relname, a.attname
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
        WHERE con.contype = 'p' AND c.relnamespace = 'public'::regnamespace
        AND format_type(a.atttypid, a.atttypmod) = 'bigint';
        """)
        bigint_pks = conn.execute(query).fetchall()
        for t, c in bigint_pks:
            try:
                print(f"Altering {t}.{c} to integer...")
                conn.execute(text(f"ALTER TABLE {t} ALTER COLUMN {c} TYPE integer;"))
                conn.commit()
                print(f"Success for {t}.{c}")
            except Exception as e:
                conn.rollback()
                print(f"Failed for {t}.{c}: {e}")

if __name__ == "__main__":
    check_db()
