import psycopg2

conn_str = "postgresql://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db"
try:
    conn = psycopg2.connect(conn_str)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("ALTER TABLE cfi_showcase_votes ADD COLUMN IF NOT EXISTS score INTEGER DEFAULT 0;")
    print("Column score added successfully.")
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error: {e}")
