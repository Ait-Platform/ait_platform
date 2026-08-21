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
        # Check if sace exists
        res_sace = conn.execute(text("SELECT id FROM auth_subject WHERE slug='sace'")).first()
        if not res_sace:
            conn.execute(text('''
                INSERT INTO auth_subject (slug, name, is_active, show_on_welcome, is_hidden_on_bridge, requires_price, commercial_mode, enroll_policy, processor_default, allow_country_pricing, mor_mode, program_type)
                VALUES ('sace', 'SACE Program Endorsement', 1, true, false, 0, 'free', 'auto_enroll', 'yoco', 0, 0, 'free')
            '''))
            print("Inserted sace")
        else:
            print("SACE exists")

        # Update about endpoints
        conn.execute(text("UPDATE auth_subject SET about_endpoint='thunee_bp.thunee_about' WHERE slug='thunee'"))
        conn.execute(text("UPDATE auth_subject SET about_endpoint='cptd_bp.cptd_about' WHERE slug='cptd'"))
        conn.execute(text("UPDATE auth_subject SET about_endpoint='sace_bp.sace_about', start_endpoint='sace_bp.dashboard' WHERE slug='sace'"))
        print("Updated about endpoints for all 3")
except Exception as e:
    print("Error:", e)
