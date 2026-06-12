import os
from sqlalchemy import create_engine, text

# Render external database URL
DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def check_db():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        # Check primary keys for auth_subject
        query = text("""
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid
                             AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = 'auth_subject'::regclass
        AND    i.indisprimary;
        """)
        result = conn.execute(query).fetchall()
        print(f"Primary keys for auth_subject: {result}")
        
        # Check all tables missing primary keys
        query_all = text("""
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
        AND n.nspname = 'public'
        AND NOT EXISTS (
            SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary
        );
        """)
        missing_pks = conn.execute(query_all).fetchall()
        print(f"Tables missing primary keys: {[r[0] for r in missing_pks]}")

if __name__ == "__main__":
    check_db()
