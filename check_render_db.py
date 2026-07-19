import psycopg2
DB_URL = 'postgres://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db'
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()
cur.execute("SELECT user_id, status, zar_amount_cents FROM user_enrollment WHERE subject_id = 12 ORDER BY id DESC LIMIT 5")
print(cur.fetchall())
