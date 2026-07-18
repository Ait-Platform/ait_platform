import sqlite3
conn = sqlite3.connect('instance/ait.db')
cur = conn.cursor()
cur.execute("SELECT id, slug, name, bypass_dashboard_endpoint, start_endpoint FROM auth_subject WHERE slug='spv';")
print(cur.fetchone())
