import sqlite3
import os

# ✅ Centralized DB connection with error handling
def get_db_connection():
    try:
        base_dir = os.path.abspath(os.path.dirname(__file__))
        db_path = os.path.join(base_dir, "data.db")
        print(f"[DB] Connecting to: {db_path}")

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        print(f"[DB ERROR] Failed to connect: {e}")
        return None

