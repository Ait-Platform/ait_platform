import sqlite3

def alter_db():
    conn = sqlite3.connect('instance/data.db')
    cursor = conn.cursor()
    
    try:
        cursor.execute("ALTER TABLE bil_property ADD COLUMN expected_water_meters INTEGER DEFAULT 0")
        print("Added expected_water_meters")
    except Exception as e:
        print(f"expected_water_meters already exists? {e}")
        
    try:
        cursor.execute("ALTER TABLE bil_property ADD COLUMN expected_elec_meters INTEGER DEFAULT 0")
        print("Added expected_elec_meters")
    except Exception as e:
        print(f"expected_elec_meters already exists? {e}")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    alter_db()
