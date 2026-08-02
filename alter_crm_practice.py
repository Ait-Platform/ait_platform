import psycopg2

DATABASE_URL = "postgresql://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def alter_crm_practice():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        cursor = conn.cursor()

        # Add wallet_balance_cents if it doesn't exist
        try:
            cursor.execute("ALTER TABLE crm_practice ADD COLUMN wallet_balance_cents INTEGER NOT NULL DEFAULT 0;")
            print("Added wallet_balance_cents")
        except psycopg2.errors.DuplicateColumn:
            print("wallet_balance_cents already exists")

        # Add trial_ends_at if it doesn't exist
        try:
            cursor.execute("ALTER TABLE crm_practice ADD COLUMN trial_ends_at TIMESTAMP;")
            print("Added trial_ends_at")
        except psycopg2.errors.DuplicateColumn:
            print("trial_ends_at already exists")

        # Add shadow_spent_cents if it doesn't exist
        try:
            cursor.execute("ALTER TABLE crm_practice ADD COLUMN shadow_spent_cents INTEGER NOT NULL DEFAULT 0;")
            print("Added shadow_spent_cents")
        except psycopg2.errors.DuplicateColumn:
            print("shadow_spent_cents already exists")

        print("Render DB migration successful!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals() and conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    alter_crm_practice()
