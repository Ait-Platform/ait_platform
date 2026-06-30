import psycopg2

db_url = "postgres://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

try:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    
    cur.execute('''
        ALTER TABLE bil_property ADD COLUMN IF NOT EXISTS onboarding_status VARCHAR(50) DEFAULT 'active';
        ALTER TABLE bil_property ADD COLUMN IF NOT EXISTS expected_bills INTEGER DEFAULT 0;
        ALTER TABLE bil_property ADD COLUMN IF NOT EXISTS expected_tenants INTEGER DEFAULT 0;
        ALTER TABLE bil_property ADD COLUMN IF NOT EXISTS is_bulk_metered INTEGER DEFAULT 0;
        ALTER TABLE bil_property ADD COLUMN IF NOT EXISTS expected_sub_meters INTEGER DEFAULT 0;
    ''')
    
    conn.commit()
    print("Successfully added columns to bil_property table!")
    
    cur.close()
    conn.close()
except Exception as e:
    print(f"Error migrating DB: {e}")
