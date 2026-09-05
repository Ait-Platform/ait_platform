from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"
engine = create_engine(DB_URL)
with engine.connect() as conn:
    res = conn.execute(text("SELECT id, name, slug FROM auth_subject WHERE is_active = 1 ORDER BY name")).fetchall()
    for r in res:
        print(r)
