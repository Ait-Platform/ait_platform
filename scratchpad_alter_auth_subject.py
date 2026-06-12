import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def check_db():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Altering auth_subject.id to integer...")
        
        # We need to temporarily drop foreign keys referencing auth_subject.id
        # Let's find them
        query_fks = text("""
        SELECT
            tc.table_name, 
            kcu.column_name, 
            tc.constraint_name 
        FROM 
            information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' 
          AND tc.table_name IN (
              SELECT relname FROM pg_class WHERE relkind = 'r'
          )
        """)
        fks = conn.execute(query_fks).fetchall()
        
        # Actually, let's just use CASCADE or explicitly drop them if needed. 
        # PostgreSQL doesn't support ALTER COLUMN ... CASCADE for type changes of primary keys easily if foreign keys exist.
        
        # Let's try direct alter.
        try:
            conn.execute(text("ALTER TABLE auth_subject ALTER COLUMN id TYPE integer;"))
            conn.commit()
            print("Successfully altered auth_subject.id to integer.")
        except Exception as e:
            conn.rollback()
            print(f"Direct alter failed: {e}")
            
            # If it fails due to foreign keys, let's see the error.

if __name__ == "__main__":
    check_db()
