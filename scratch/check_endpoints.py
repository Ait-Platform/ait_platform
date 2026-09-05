from sqlalchemy import create_engine, text
DB_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"
engine = create_engine(DB_URL)
with engine.begin() as conn:
    res = conn.execute(text("SELECT bypass_dashboard_endpoint, start_endpoint FROM auth_subject WHERE id = 42")).fetchone()
    print("endpoints:", res)
