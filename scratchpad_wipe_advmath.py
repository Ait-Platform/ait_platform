import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def wipe():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Wiping adv_math for all@gmail.com...")
        conn.execute(text("""
            DELETE FROM user_enrollment 
            WHERE user_id = (SELECT id FROM "user" WHERE email = 'all@gmail.com')
            AND subject_id = (SELECT id FROM auth_subject WHERE slug = 'adv_math');
        """))
        conn.commit()
        print("Done.")

if __name__ == "__main__":
    wipe()
