import psycopg2
conn = psycopg2.connect('postgresql://ait_local:temp1234@localhost:5432/ait_local_db')
cur = conn.cursor()
cur.execute("SELECT table_type FROM information_schema.tables WHERE table_name='rdp_enrollment';")
res = cur.fetchone()
print(res[0] if res else 'Table not found')
