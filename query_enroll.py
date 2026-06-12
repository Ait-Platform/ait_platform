import psycopg2
conn = psycopg2.connect('postgresql://ait_local:temp1234@localhost:5432/ait_local_db')
cur = conn.cursor()
cur.execute("SELECT slug, enroll_policy FROM auth_subject")
print(cur.fetchall())
