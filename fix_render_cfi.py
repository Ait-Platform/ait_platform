import psycopg2
from datetime import datetime

# Render DB Connection
DB_URL = "postgres://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def fix_render_cfi_prices():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    
    # Get all countries and their fx_to_zar rates
    cur.execute("SELECT alpha2, currency, fx_to_zar FROM ref_country_currency")
    countries = cur.fetchall()
    
    insert_queries = []
    
    for alpha2, currency, fx_to_zar in countries:
        # Base is 100 local currency (10000 cents)
        local_amount_cents = 10000
        
        # Calculate ZAR equivalent
        if currency == "ZAR" or alpha2 == "ZA":
            zar_amount_cents = 10000
        else:
            if fx_to_zar is None:
                # Fallback if no FX rate is available, default to 10000 just in case
                fx_to_zar = 1.0 
            zar_amount_cents = max(1, int(local_amount_cents * float(fx_to_zar)))
            
        insert_queries.append(f"""
        INSERT INTO subject_country_price 
        (subject_id, country_code, local_amount_cents, zar_amount_cents, local_currency, price_version, is_active, created_at)
        VALUES (12, '{alpha2}', {local_amount_cents}, {zar_amount_cents}, '{currency}', '1', true, NOW())
        ON CONFLICT (subject_id, country_code) 
        DO UPDATE SET 
            local_amount_cents = EXCLUDED.local_amount_cents, 
            zar_amount_cents = EXCLUDED.zar_amount_cents,
            local_currency = EXCLUDED.local_currency;
        """)
        
    for q in insert_queries:
        cur.execute(q)
        
    conn.commit()
    cur.close()
    conn.close()
    print(f"Successfully updated CFI prices for {len(insert_queries)} countries in the Render Database!")

if __name__ == "__main__":
    fix_render_cfi_prices()
