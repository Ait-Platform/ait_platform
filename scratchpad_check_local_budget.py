import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db"

def check():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("\nChecking enrollments for budget:")
        res = conn.execute(text("""
            SELECT e.id, u.email, s.slug, e.status
            FROM user_enrollment e
            JOIN "user" u ON u.id = e.user_id
            JOIN auth_subject s ON s.id = e.subject_id
            WHERE s.slug = 'budget' AND u.email = 'all@gmail.com'
        """)).mappings().all()
        for r in res:
            print(dict(r))

if __name__ == "__main__":
    check()
