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
    with pg_engine.begin() as conn:
        conn.execute(text("UPDATE auth_subject SET start_endpoint='thunee_bp.index' WHERE slug='thunee'"))
        conn.execute(text("UPDATE auth_subject SET start_endpoint='cptd_bp.index' WHERE slug='cptd'"))
        print("Updated endpoints")
except Exception as e:
    print("Error:", e)
