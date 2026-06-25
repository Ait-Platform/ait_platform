import sqlite3
import os

db_path = os.path.join(os.path.dirname(__file__), 'instance', 'data.db')
print(f"Connecting to {db_path}...")

conn = sqlite3.connect(db_path)
try:
    conn.execute('ALTER TABLE crm_enquiry ADD COLUMN patient_id_no VARCHAR(50)')
    print("Added patient_id_no")
except sqlite3.OperationalError as e:
    print(e)
    
try:
    conn.execute('ALTER TABLE crm_enquiry ADD COLUMN medical_aid_no VARCHAR(100)')
    print("Added medical_aid_no")
except sqlite3.OperationalError as e:
    print(e)

conn.commit()
conn.close()
print("Done.")
