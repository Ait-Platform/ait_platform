from sqlalchemy import create_engine, text

# Render URL from .env
DB_URL = "postgresql+psycopg2://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

engine = create_engine(DB_URL)
with engine.begin() as conn:
    # Check if sace_reading exists
    res = conn.execute(text("SELECT id FROM auth_subject WHERE slug = 'sace_reading'")).fetchone()
    
    if not res:
        # Insert it
        conn.execute(text("""
            INSERT INTO auth_subject (slug, name, is_active, sort_order, trial_days, commercial_mode, enroll_policy, processor_default, requires_price, allow_country_pricing, mor_mode, program_type, is_hidden_on_bridge, show_on_welcome)
            VALUES ('sace_reading', 'SACE Reading Workshop', 1, 100, 0, 'paid', 'auto_enroll', 'paystack', 1, 1, 0, 'course', false, false)
        """))
        print("Inserted sace_reading subject")
        
        # We also need to add a price for it in auth_pricing if possible, but the admin usually does that through the UI.
    else:
        print("sace_reading already exists")
