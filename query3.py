import psycopg2
conn = psycopg2.connect('postgresql://ait_local:temp1234@localhost:5432/ait_local_db')
cur = conn.cursor()
cur.execute("SELECT view_definition FROM information_schema.views WHERE table_name='rdp_enrollment';")
res = cur.fetchone()
print(res[0] if res else 'View not found')
