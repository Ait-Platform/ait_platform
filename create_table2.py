import psycopg2

db_url = "postgres://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

try:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS bil_extraction_log (
            id SERIAL PRIMARY KEY,
            manager_id INTEGER NOT NULL REFERENCES "user"(id),
            property_name VARCHAR(255),
            address TEXT,
            metro_account_no VARCHAR(100),
            muni_email VARCHAR(255),
            has_rates BOOLEAN,
            rates_amount FLOAT,
            amount_due FLOAT,
            raw_json JSON,
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit()
    print("Created bil_extraction_log on Render.")
    cur.close()
    conn.close()
except Exception as e:
    print(f"Error creating table: {e}")
