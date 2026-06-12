import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def check_db():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Columns in stripe_payment:")
        query = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'stripe_payment';
        """)
        cols = conn.execute(query).fetchall()
        for c in cols:
            print(c[0])
            
        print("\nChecking recent enrollments:")
        query2 = text("""
        SELECT e.id, e.user_id, u.email, s.slug, e.status, e.created_at
        FROM user_enrollment e
        JOIN "user" u ON u.id = e.user_id
        JOIN auth_subject s ON s.id = e.subject_id
        ORDER BY e.created_at DESC
        LIMIT 5;
        """)
        enrollments = conn.execute(query2).fetchall()
        for e in enrollments:
            print(f"Enrollment: {e}")

if __name__ == "__main__":
    check_db()
