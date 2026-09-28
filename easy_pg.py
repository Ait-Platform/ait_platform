import psycopg2
conn = psycopg2.connect('postgresql://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db')
cur = conn.cursor()

cur.execute("SELECT id, name FROM core_organization WHERE name ILIKE '%%Manor%%'")
org_id, org_name = cur.fetchone()

print("--- MO Appointments in Committee ---")
cur.execute("SELECT id, name, email, user_id, status FROM uip_committee_member WHERE organization_id=%s AND position ILIKE '%%Municipal%%'", (org_id,))
for row in cur.fetchall():
    print(row)

print("\n--- Check uipmo@gmail.com ---")
cur.execute("SELECT id, email FROM \"user\" WHERE email = 'uipmo@gmail.com'")
row = cur.fetchone()
if row:
    print(row)
else:
    print("Not found")
