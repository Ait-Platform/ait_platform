from sqlalchemy import create_engine, text

# Render URL from .env
DB_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

engine = create_engine(DB_URL)
with engine.begin() as conn:
    conn.execute(text("UPDATE auth_subject SET about_endpoint = 'sace_bp.sace_about' WHERE slug = 'sace_hub'"))
    print("Successfully updated Render DB")
