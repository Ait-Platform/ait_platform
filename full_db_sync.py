import os
from sqlalchemy import create_engine, MetaData, Table, text

LOCAL_URL = "postgresql+psycopg2://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db"
PG_URL = (
    "postgresql+psycopg2://"
    "ait_platform_db_user:"
    "b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj"
    "@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432"
    "/ait_platform_db"
)

local_engine = create_engine(LOCAL_URL)
pg_engine = create_engine(PG_URL)

metadata = MetaData()
metadata.reflect(bind=local_engine)

print("Starting full DB sync from Local to Render...")

# Create missing tables
metadata.create_all(pg_engine)

# It's important to disable foreign key checks, or delete in the correct order.
# For simplicity, we just delete everything and insert.
with pg_engine.begin() as p_conn:
    skip_tables = {'budget_ledger', 'subject_country_price_bak_20251226', 'tmp_country_price', 'alembic_version'}
    table_names = [f'"{table.name}"' for table in metadata.sorted_tables if table.name not in skip_tables]
    if table_names:
        truncate_sql = f"TRUNCATE TABLE {', '.join(table_names)} CASCADE"
        p_conn.execute(text(truncate_sql))
        print("Truncated all tables on Render.")

# Insert in topological order to satisfy foreign keys
for table in metadata.sorted_tables:
    table_name = table.name
    if table_name in skip_tables:
        print(f"Skipping insertion for {table_name}")
        continue
    print(f"Syncing {table_name}...")
    try:
        with local_engine.connect() as l_conn:
            data = [dict(row) for row in l_conn.execute(table.select()).mappings().all()]
        if not data:
            print(f"  No data for {table_name}, skipping.")
            continue
            
        with pg_engine.begin() as p_conn:
            p_conn.execute(table.insert(), data)
        print(f"  Synced {len(data)} rows for {table_name}")
    except Exception as e:
        print(f"  Error syncing {table_name}: {e}")

print("Sync Complete!")
