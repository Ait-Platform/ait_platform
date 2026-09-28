import psycopg2
conn = psycopg2.connect('postgresql://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db')
cur = conn.cursor()
cur.execute("SELECT id, name, slug, organization_id FROM core_role")
roles = cur.fetchall()
for r in roles:
    print(r)
