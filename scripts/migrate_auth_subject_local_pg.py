import os
import sys

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://postgres:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@localhost:5432/ait_local_db"

def migrate():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        # 1. Add columns to auth_subject table
        try:
            conn.execute(text("ALTER TABLE auth_subject ADD COLUMN is_hidden_on_bridge BOOLEAN NOT NULL DEFAULT FALSE;"))
            conn.execute(text("ALTER TABLE auth_subject ADD COLUMN parent_subject_id INTEGER REFERENCES auth_subject(id);"))
            conn.execute(text("ALTER TABLE auth_subject ADD COLUMN bypass_dashboard_endpoint VARCHAR(128);"))
            conn.commit()
            print(f"Added columns to auth_subject on {DATABASE_URL}.")
        except Exception as e:
            conn.rollback()
            print(f"Columns might already exist or error: {e}")

        # 2. Backfill existing rules
        print("Backfilling rules...")
        
        # is_hidden_on_bridge
        conn.execute(text("""
            UPDATE auth_subject 
            SET is_hidden_on_bridge = TRUE 
            WHERE slug IN ('home_premium', 'home2', 'home_section3', 'cfi_judge');
        """))

        # parent_subject_id
        conn.execute(text("""
            UPDATE auth_subject 
            SET parent_subject_id = (SELECT id FROM auth_subject WHERE slug = 'home') 
            WHERE slug IN ('home_premium', 'home2', 'home_section3');
        """))

        # bypass_dashboard_endpoint
        bypasses = {
            'cultural_fire': 'cultural_bp.cultural_fire_router',
            'sms': 'sms_bp.sms_entry',
            'budget': 'budget_bp.dashboard',
            'reading': 'reading_bp.subject_home',
            'adv_math': 'adv_math_bp.dashboard'
        }
        
        for slug, endpoint in bypasses.items():
            conn.execute(text("""
                UPDATE auth_subject 
                SET bypass_dashboard_endpoint = :endpoint 
                WHERE slug = :slug;
            """), {"endpoint": endpoint, "slug": slug})

        # Set start_endpoint for loss and home (for the "Press Next" screen)
        start_endpoints = {
            'loss': 'loss_bp.subject_home',
            'home': 'home_bp.learner_dashboard',
            'billing': 'billing_bp.learner_dashboard'
        }
        for slug, endpoint in start_endpoints.items():
            conn.execute(text("""
                UPDATE auth_subject 
                SET start_endpoint = :endpoint 
                WHERE slug = :slug;
            """), {"endpoint": endpoint, "slug": slug})

        conn.commit()
        print("Backfill complete.")

if __name__ == "__main__":
    migrate()
