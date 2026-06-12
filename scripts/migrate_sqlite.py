import sqlite3

def migrate():
    conn = sqlite3.connect('db.sqlite3')
    cursor = conn.cursor()
    
    # 1. Add columns to auth_subject table
    try:
        cursor.execute("ALTER TABLE auth_subject ADD COLUMN is_hidden_on_bridge BOOLEAN NOT NULL DEFAULT 0;")
        cursor.execute("ALTER TABLE auth_subject ADD COLUMN parent_subject_id INTEGER REFERENCES auth_subject(id);")
        cursor.execute("ALTER TABLE auth_subject ADD COLUMN bypass_dashboard_endpoint VARCHAR(128);")
        conn.commit()
        print("Added columns to auth_subject.")
    except Exception as e:
        conn.rollback()
        print(f"Columns might already exist or error: {e}")

    # 2. Backfill existing rules
    print("Backfilling rules...")
    
    # is_hidden_on_bridge
    cursor.execute("""
        UPDATE auth_subject 
        SET is_hidden_on_bridge = 1 
        WHERE slug IN ('home_premium', 'home2', 'home_section3', 'cfi_judge');
    """)

    # parent_subject_id
    cursor.execute("""
        UPDATE auth_subject 
        SET parent_subject_id = (SELECT id FROM auth_subject WHERE slug = 'home') 
        WHERE slug IN ('home_premium', 'home2', 'home_section3');
    """)

    # bypass_dashboard_endpoint
    bypasses = {
        'cultural_fire': 'cultural_bp.cultural_fire_router',
        'sms': 'sms_bp.sms_entry',
        'budget': 'budget_bp.dashboard',
        'reading': 'reading_bp.subject_home',
        'adv_math': 'adv_math_bp.dashboard'
    }
    
    for slug, endpoint in bypasses.items():
        cursor.execute("""
            UPDATE auth_subject 
            SET bypass_dashboard_endpoint = ? 
            WHERE slug = ?;
        """, (endpoint, slug))

    # Set start_endpoint for loss and home (for the "Press Next" screen)
    start_endpoints = {
        'loss': 'loss_bp.subject_home',
        'home': 'home_bp.learner_dashboard',
        'billing': 'billing_bp.learner_dashboard'
    }
    for slug, endpoint in start_endpoints.items():
        cursor.execute("""
            UPDATE auth_subject 
            SET start_endpoint = ? 
            WHERE slug = ?;
        """, (endpoint, slug))

    conn.commit()
    conn.close()
    print("Backfill complete.")

if __name__ == "__main__":
    migrate()
