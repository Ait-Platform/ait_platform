import psycopg2
from datetime import datetime

# Local DB Connection
DB_URL = "postgresql+psycopg2://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db"

def fix_local_cfi_prices():
    conn = psycopg2.connect("dbname=ait_local_db user=postgres password=password host=localhost")
    cur = conn.cursor()
    
    cur.execute("SELECT alpha2, currency, fx_to_zar FROM ref_country_currency")
    countries = cur.fetchall()
    
    insert_queries = []
    
    for alpha2, currency, fx_to_zar in countries:
        local_amount_cents = 10000
        if currency == "ZAR" or alpha2 == "ZA":
            zar_amount_cents = 10000
        else:
            if fx_to_zar is None:
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
    print(f"Successfully updated CFI prices for {len(insert_queries)} countries in the Local Database!")

if __name__ == "__main__":
    fix_local_cfi_prices()
