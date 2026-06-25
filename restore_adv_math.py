from sqlalchemy import create_engine, text

LOCAL_URI = 'postgresql+psycopg2://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db'
RENDER_URI = 'postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db'

for name, uri in [('Local', LOCAL_URI), ('Render', RENDER_URI)]:
    print(f'Connecting to {name}...')
    try:
        engine = create_engine(uri)
        with engine.begin() as conn:
            # Check if it already exists
            row = conn.execute(text("SELECT id FROM auth_subject WHERE slug='adv_math'")).fetchone()
            if row:
                print(f"  adv_math already exists with id {row[0]}")
                continue

            sql = """
                INSERT INTO auth_subject (
                    id, slug, name, is_active, sort_order, trial_days,
                    commercial_mode, enroll_policy, processor_default,
                    requires_price, allow_country_pricing, mor_mode,
                    program_type, start_endpoint, about_endpoint, pay_endpoint, admin_start_endpoint
                ) VALUES (
                    21, 'adv_math', 'Adv Math', 1, 30, 0,
                    'paid', 'post_payment', 'yoco',
                    1, 1, 0,
                    'paid', 'adv_math_bp.dashboard', 'adv_math_bp.about', 'yoco_bp.yoco_start', 'admin_bp.admin_home'
                )
            """
            conn.execute(text(sql))
            print(f"  Restored adv_math to {name}!")
    except Exception as e:
        print(f"  Failed on {name}: {e}")
