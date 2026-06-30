import psycopg2
import json

db_url = "postgres://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

try:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute('''
        SELECT id, property_name, address, rates_amount, amount_due, created_at 
        FROM bil_extraction_log 
        ORDER BY created_at DESC 
        LIMIT 5;
    ''')
    rows = cur.fetchall()
    
    if not rows:
        print("No records found in bil_extraction_log yet.")
    else:
        print(f"Found {len(rows)} records:")
        for r in rows:
            print(f"ID: {r[0]} | Property: {r[1]} | Address: {r[2]} | Rates: {r[3]} | Due: {r[4]} | Time: {r[5]}")
            
    cur.close()
    conn.close()
except Exception as e:
    print(f"Error querying table: {e}")
