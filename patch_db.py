import sqlite3
try:
    conn = sqlite3.connect("your_database.db")
    c = conn.cursor()
    c.execute("ALTER TABLE core_interaction ADD COLUMN parent_id INTEGER REFERENCES core_interaction(id);")
    conn.commit()
    conn.close()
    print("Added column")
except Exception as e:
    print(e)
