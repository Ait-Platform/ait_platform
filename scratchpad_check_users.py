import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def check():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("\nChecking users for all@gmail.com:")
        res = conn.execute(text("""
            SELECT id, email
            FROM "user"
            WHERE email ILIKE '%all@gmail.com%'
        """)).mappings().all()
        for r in res:
            print(dict(r))

if __name__ == "__main__":
    check()
