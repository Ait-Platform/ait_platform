import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def disable_adv_math():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Disabling adv_math in auth_subject...")
        conn.execute(text("""
            UPDATE auth_subject 
            SET is_active = 0 
            WHERE slug = 'adv_math';
        """))
        conn.commit()
        print("Done. adv_math is now inactive.")

if __name__ == "__main__":
    disable_adv_math()
