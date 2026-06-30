import psycopg2

db_url = "postgres://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

try:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute("UPDATE alembic_version SET version_num = '45629455c046';")
    conn.commit()
    print("Updated alembic_version on Render.")
    cur.close()
    conn.close()
except Exception as e:
    print(f"Error: {e}")
