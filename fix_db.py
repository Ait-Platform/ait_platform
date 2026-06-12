import psycopg2
conn = psycopg2.connect('postgresql://ait_local:temp1234@localhost:5432/ait_local_db')
cur = conn.cursor()
cur.execute("UPDATE auth_subject SET pay_endpoint = 'yoco_bp.yoco_start' WHERE pay_endpoint = 'payment_bp.checkout_review';")
conn.commit()
print('Rows updated:', cur.rowcount)
