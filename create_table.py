import os
import psycopg2

db_url = os.environ.get("RENDER_DB_URL")
if not db_url:
    # Just in case they have it in .env
    from dotenv import load_dotenv
    load_dotenv()
    db_url = os.environ.get("RENDER_DB_URL")

if db_url:
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
else:
    print("No RENDER_DB_URL found.")
