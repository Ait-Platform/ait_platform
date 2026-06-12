import sqlite3

def check_db():
    conn = sqlite3.connect('data.db')
    cur = conn.cursor()
    cur.execute("SELECT slug, commercial_mode FROM auth_subject")
    rows = cur.fetchall()
    for row in rows:
        print(row)
    conn.close()

if __name__ == "__main__":
    check_db()
