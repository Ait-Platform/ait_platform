import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def update_db():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Updating loss subject to paid...")
        conn.execute(text("""
        UPDATE auth_subject 
        SET program_type = 'paid', commercial_mode = 'paid'
        WHERE slug = 'loss';
        """))
        conn.commit()
        print("Successfully updated loss to paid.")

if __name__ == "__main__":
    update_db()
