import psycopg2
conn = psycopg2.connect('postgresql://ait_local:temp1234@localhost:5432/ait_local_db')
cur = conn.cursor()
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='rdp_enrollment';")
print(cur.fetchall())
