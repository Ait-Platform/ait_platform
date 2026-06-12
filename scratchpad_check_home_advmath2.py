import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def check_db():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Checking commercial mode for 'home':")
        query = text("""
        SELECT slug, commercial_mode, program_type, requires_price FROM auth_subject WHERE slug = 'home';
        """)
        home_subj = conn.execute(query).fetchone()
        print(f"Home Subject: {home_subj}")
        
        print("\nChecking enrollments for all@gmail.com:")
        query2 = text("""
        SELECT e.id, s.slug, e.status
        FROM user_enrollment e
        JOIN "user" u ON u.id = e.user_id
        JOIN auth_subject s ON s.id = e.subject_id
        WHERE u.email = 'all@gmail.com'
        ORDER BY e.id DESC;
        """)
        enrollments = conn.execute(query2).fetchall()
        for e in enrollments:
            print(f"Enrollment: {e}")

if __name__ == "__main__":
    check_db()
