import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def check():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Checking budget subject:")
        res = conn.execute(text("SELECT id, slug, name, commercial_mode, program_type, requires_price, trial_days FROM auth_subject WHERE slug = 'budget'")).mappings().fetchone()
        print(dict(res))

        print("\nChecking enrollments for all@gmail.com:")
        res = conn.execute(text("""
            SELECT e.id, s.slug, e.status
            FROM user_enrollment e
            JOIN "user" u ON u.id = e.user_id
            JOIN auth_subject s ON s.id = e.subject_id
            WHERE u.email = 'all@gmail.com'
        """)).mappings().all()
        for r in res:
            print(dict(r))

if __name__ == "__main__":
    check()
