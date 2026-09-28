import sqlite3
conn = sqlite3.connect('instance/data.db')
cur = conn.cursor()
cur.execute("SELECT * FROM auth_subject")
print(cur.fetchall())
