from sqlalchemy import create_engine, text

PG_URL = (
    "postgresql+psycopg2://"
    "ait_platform_db_user:"
    "b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj"
    "@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432"
    "/ait_platform_db"
)
pg_engine = create_engine(PG_URL)

try:
    with pg_engine.begin() as p_conn:
        res = p_conn.execute(text("SELECT * FROM mech_communications ORDER BY id DESC LIMIT 5"))
        rows = res.fetchall()
        for r in rows:
            print(dict(r._mapping))
        if not rows:
            print("No records found in mech_communications table.")
except Exception as e:
    print("Error:", e)
