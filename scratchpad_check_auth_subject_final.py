import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def check_db():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Checking auth_subject.id type:")
        query = text("""
        SELECT format_type(atttypid, atttypmod)
        FROM pg_attribute
        WHERE attrelid = 'auth_subject'::regclass AND attname = 'id';
        """)
        res = conn.execute(query).fetchone()
        print(f"auth_subject.id type: {res[0] if res else 'None'}")
        
        print("Checking user.id type:")
        query2 = text("""
        SELECT format_type(atttypid, atttypmod)
        FROM pg_attribute
        WHERE attrelid = '"user"'::regclass AND attname = 'id';
        """)
        res2 = conn.execute(query2).fetchone()
        print(f"user.id type: {res2[0] if res2 else 'None'}")

        print("Checking tables missing primary key constraints:")
        query3 = text("""
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
        AND n.nspname = 'public'
        AND NOT EXISTS (
            SELECT 1 FROM pg_constraint con WHERE con.conrelid = c.oid AND con.contype = 'p'
        );
        """)
        missing = conn.execute(query3).fetchall()
        print(f"Tables without PK constraint: {[r[0] for r in missing]}")

if __name__ == "__main__":
    check_db()
