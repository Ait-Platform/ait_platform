from sqlalchemy import create_engine, text

PG_URL = (
    "postgresql+psycopg2://"
    "ait_platform_db_user:"
    "b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj"
    "@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432"
    "/ait_platform_db"
)

pg_engine = create_engine(PG_URL)

print("Connecting to remote Render Postgres database...")

try:
    with pg_engine.begin() as p_conn:
        p_conn.execute(text('ALTER TABLE mech_vehicles ADD COLUMN IF NOT EXISTS engine_no VARCHAR(50);'))
        p_conn.execute(text('ALTER TABLE mech_vehicles ADD COLUMN IF NOT EXISTS gvm VARCHAR(20);'))
        p_conn.execute(text('ALTER TABLE mech_vehicles ADD COLUMN IF NOT EXISTS tare VARCHAR(20);'))
        p_conn.execute(text('ALTER TABLE mech_vehicles ADD COLUMN IF NOT EXISTS disk_license_no VARCHAR(50);'))
    print("SUCCESS: Added new AI vehicle columns to remote database.")
except Exception as e:
    print(f"ERROR: {e}")
