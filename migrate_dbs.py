import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def run_migrations_for_uri(uri, name):
    print(f"--- Migrating {name} ---")
    if not uri or not uri.startswith('postgres'):
        print(f"SKIPPING: Invalid or missing Postgres URI for {name}.")
        return
        
    try:
        conn = psycopg2.connect(uri)
        conn.autocommit = True
        cur = conn.cursor()
        
        commands = [
            "ALTER TABLE sender_profile ADD COLUMN IF NOT EXISTS letterhead_url VARCHAR(255);",
            "ALTER TABLE sender_profile ADD COLUMN IF NOT EXISTS use_custom_letterhead BOOLEAN DEFAULT FALSE;"
        ]
        
        for cmd in commands:
            try:
                cur.execute(cmd)
                print(f"SUCCESS: {cmd}")
            except Exception as e:
                print(f"SKIPPED (already exists?): {cmd}")
                
        cur.close()
        conn.close()
        print(f"Finished {name}\n")
    except Exception as e:
        print(f"Could not connect to {name}. Error: {e}\n")

if __name__ == '__main__':
    # 1. Migrate Local DB
    local_uri = os.getenv("FLASK_SQLALCHEMY_DATABASE_URI")
    if not local_uri:
        # fallback for local
        local_uri = "postgresql://postgres:password@localhost:5432/ait_local_db" 
    run_migrations_for_uri(local_uri, "Local Database (ait_local_db)")
    
    # 2. Migrate Render DB
    render_uri = os.getenv("RENDER_DATABASE_URI") # They can set this in .env
    if not render_uri:
        print("Note: To migrate the Render DB, please add RENDER_DATABASE_URI to your .env file")
        print("Or enter the External Database URL from your Render dashboard below:")
        render_uri = input("Render DB URI (leave blank to skip): ").strip()
        
    if render_uri:
        run_migrations_for_uri(render_uri, "Render Database (ait_platform_db)")
