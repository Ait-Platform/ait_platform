import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def fix_db():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        # Find tables without a primary key constraint
        query = text("""
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
        AND n.nspname = 'public'
        AND NOT EXISTS (
            SELECT 1 FROM pg_constraint con WHERE con.conrelid = c.oid AND con.contype = 'p'
        );
        """)
        missing_pks = conn.execute(query).fetchall()
        tables_missing_pk = [r[0] for r in missing_pks]
        print(f"Tables missing primary key constraints: {tables_missing_pk}")

        for table in tables_missing_pk:
            # We assume the primary key should be 'id'. Let's check if 'id' exists.
            query_col = text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}' AND column_name = 'id'")
            has_id = conn.execute(query_col).fetchone()
            if has_id:
                try:
                    # Check if the unique index table_pkey exists
                    index_name = f"{table}_pkey"
                    query_idx = text(f"SELECT 1 FROM pg_class WHERE relname = '{index_name}'")
                    has_idx = conn.execute(query_idx).fetchone()
                    
                    if has_idx:
                        print(f"Adding primary key using existing index for {table}")
                        conn.execute(text(f"ALTER TABLE {table} ADD PRIMARY KEY USING INDEX {index_name};"))
                    else:
                        print(f"Adding primary key constraint to {table} on id")
                        conn.execute(text(f"ALTER TABLE {table} ADD PRIMARY KEY (id);"))
                    conn.commit()
                    print(f"Successfully added PK to {table}")
                except Exception as e:
                    conn.rollback()
                    print(f"Failed to add PK to {table}: {e}")

if __name__ == "__main__":
    fix_db()
