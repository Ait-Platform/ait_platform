import os
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def check_db():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        print("Checking active locks:")
        query = text("""
        SELECT l.pid, l.mode, l.granted, l.relation::regclass, a.query
        FROM pg_locks l
        JOIN pg_stat_activity a ON l.pid = a.pid
        WHERE l.relation IS NOT NULL
        AND a.pid <> pg_backend_pid();
        """)
        locks = conn.execute(query).fetchall()
        for lock in locks:
            print(f"Lock: {lock}")
            
        print("Checking stuck queries:")
        query2 = text("""
        SELECT pid, state, wait_event_type, wait_event, query
        FROM pg_stat_activity
        WHERE state != 'idle' AND pid <> pg_backend_pid();
        """)
        queries = conn.execute(query2).fetchall()
        for q in queries:
            print(f"Query: {q}")

if __name__ == "__main__":
    check_db()
