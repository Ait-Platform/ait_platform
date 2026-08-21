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
        res = p_conn.execute(text("UPDATE ait_token_transaction SET description = 'Generated quote (Legacy)' WHERE description LIKE 'Generated quote for shop %'"))
        print(f"Updated {res.rowcount} legacy transactions on Render.")
except Exception as e:
    print("Error:", e)
