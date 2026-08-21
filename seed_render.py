from sqlalchemy import create_engine, text
import datetime

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
        # Check if thunee exists
        res_thunee = conn.execute(text("SELECT id FROM auth_subject WHERE slug='thunee'")).first()
        if not res_thunee:
            conn.execute(text('''
                INSERT INTO auth_subject (slug, name, is_active, show_on_welcome, is_hidden_on_bridge, requires_price, commercial_mode, enroll_policy, processor_default, allow_country_pricing, mor_mode, program_type)
                VALUES ('thunee', 'Thunee Game', 1, true, false, 0, 'free', 'auto_enroll', 'yoco', 0, 0, 'free')
            '''))
            print("Inserted thunee")
        else:
            print("Thunee exists")

        # Check if cptd exists
        res_cptd = conn.execute(text("SELECT id FROM auth_subject WHERE slug='cptd'")).first()
        if not res_cptd:
            conn.execute(text('''
                INSERT INTO auth_subject (slug, name, is_active, show_on_welcome, is_hidden_on_bridge, requires_price, commercial_mode, enroll_policy, processor_default, allow_country_pricing, mor_mode, program_type)
                VALUES ('cptd', 'SACE Compliance (CPTD)', 1, true, false, 0, 'free', 'auto_enroll', 'yoco', 0, 0, 'free')
            '''))
            print("Inserted cptd")
        else:
            print("CPTD exists")
except Exception as e:
    print("Error:", e)
