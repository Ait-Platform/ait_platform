import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def check_db():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Tables in DB:")
        query = text("""
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public';
        """)
        tables = conn.execute(query).fetchall()
        for t in tables:
            print(t[0])

if __name__ == "__main__":
    check_db()
