from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"
engine = create_engine(DB_URL)
with engine.connect() as conn:
    res = conn.execute(text("SELECT data_type FROM information_schema.columns WHERE table_name = 'auth_subject' AND column_name = 'is_active'")).fetchone()
    print("is_active type:", res)
