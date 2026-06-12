from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()
with app.app_context():
    # Insert subject if it doesn't exist
    subject_exists = db.session.execute(text("SELECT id FROM auth_subject WHERE slug='home_section3'")).fetchone()
    
    if not subject_exists:
        # Get next sort order
        max_sort = db.session.execute(text("SELECT MAX(sort_order) FROM auth_subject")).scalar() or 0
        db.session.execute(text("""
            INSERT INTO auth_subject (name, slug, commercial_mode, program_type, sort_order, is_active)
            VALUES ('HOME Section 3 Upgrade', 'home_section3', 'paid', 'course', :sort, 1)
        """), {"sort": max_sort + 10})
        
        subject_id = db.session.execute(text("SELECT id FROM auth_subject WHERE slug='home_section3'")).scalar()
        
        # Insert pricing
        db.session.execute(text("""
            INSERT INTO auth_pricing (subject_id, role, plan, currency, amount_cents, is_active, active_from)
            VALUES (:sid, 'user', 'enrollment', 'ZAR', 25000, 1, CURRENT_TIMESTAMP)
        """), {"sid": subject_id})
        
        db.session.commit()
        print(f"Created home_section3 with ID {subject_id} and added pricing.")
    else:
        print("home_section3 already exists!")
