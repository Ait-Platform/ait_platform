import psycopg2
import json

db_url = "postgres://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

try:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute('''
        SELECT id, name, address, enrollment_id
        FROM bil_property 
        ORDER BY id DESC 
        LIMIT 5;
    ''')
    rows = cur.fetchall()
    
    if not rows:
        print("No properties found.")
    else:
        for r in rows:
            print(f"ID: {r[0]} | Name: {r[1]} | Addr: {r[2]} | Enrollment: {r[3]}")
            
    cur.close()
    conn.close()
except Exception as e:
    print(f"Error querying table: {e}")
