import psycopg2

db_url = "postgres://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

try:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    
    # Get subject IDs
    cur.execute("SELECT id, slug FROM auth_subject WHERE slug IN ('budget', 'mechanic');")
    subjects = cur.fetchall()
    
    budget_id = None
    mechanic_id = None
    
    for s_id, slug in subjects:
        if slug == 'budget': budget_id = s_id
        if slug == 'mechanic': mechanic_id = s_id
        
    if not budget_id or not mechanic_id:
        print("Could not find both subjects.")
    else:
        # Copy prices from budget to mechanic
        cur.execute('''
            INSERT INTO subject_country_price (subject_id, country_code, local_amount_cents, local_currency, zar_amount_cents, fx_rate, is_discount, created_at, updated_at)
            SELECT %s, country_code, local_amount_cents, local_currency, zar_amount_cents, fx_rate, is_discount, created_at, updated_at
            FROM subject_country_price
            WHERE subject_id = %s
            ON CONFLICT (subject_id, country_code) DO NOTHING
            RETURNING id;
        ''', (mechanic_id, budget_id))
        
        inserted = cur.fetchall()
        conn.commit()
        print(f"Copied {len(inserted)} pricing rows from budget to mechanic CRM.")
        
    cur.close()
    conn.close()
except Exception as e:
    print(f"Error copying prices: {e}")
