import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def check_db():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Checking auth_subject in all schemas:")
        query = text("""
        SELECT n.nspname, c.relname, c.oid
        FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relname = 'auth_subject';
        """)
        schemas = conn.execute(query).fetchall()
        for r in schemas:
            print(f"Schema/Table: {r}")
            
        print("Checking primary key index validity:")
        query2 = text("""
        SELECT indisvalid 
        FROM pg_index 
        WHERE indrelid = 'public.auth_subject'::regclass AND indisprimary;
        """)
        valid = conn.execute(query2).fetchall()
        print(f"Valid PK index: {valid}")

if __name__ == "__main__":
    check_db()
