import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def check_db():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        # Check all constraints on auth_subject
        query = text("""
        SELECT conname, contype, pg_get_constraintdef(c.oid)
        FROM pg_constraint c
        JOIN pg_class t ON c.conrelid = t.oid
        WHERE t.relname = 'auth_subject';
        """)
        result = conn.execute(query).fetchall()
        for r in result:
            print(f"Constraint: {r}")

        # Check indexes
        query2 = text("""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = 'auth_subject';
        """)
        result2 = conn.execute(query2).fetchall()
        for r in result2:
            print(f"Index: {r}")

if __name__ == "__main__":
    check_db()
