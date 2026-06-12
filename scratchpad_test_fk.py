import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def test_fk():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Testing direct FK creation...")
        try:
            conn.execute(text("""
            CREATE TABLE test_user_enrollment (
                id SERIAL NOT NULL,
                user_id INTEGER NOT NULL,
                subject_id INTEGER NOT NULL,
                PRIMARY KEY (id),
                FOREIGN KEY(subject_id) REFERENCES auth_subject (id)
            );
            """))
            conn.commit()
            print("Successfully created dummy table with FK.")
            conn.execute(text("DROP TABLE test_user_enrollment;"))
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Failed to create dummy table: {e}")

if __name__ == "__main__":
    test_fk()
