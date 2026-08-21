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
        p_conn.execute(text("""
            INSERT INTO invite_log (sender_id, recipient_phone, program_slug, invite_type, status, sent_at)
            VALUES (1, 'Sk (Client)', 'mechanic', 'Email Quote #JOB-8EF3D9', 'Sent', CURRENT_TIMESTAMP)
        """))
        print("Successfully injected dummy log.")
except Exception as e:
    print("Error:", e)
