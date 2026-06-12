import os
import sys

# Add path to load app
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app()

with app.app_context():
    # Use raw SQL to insert pricing to avoid ORM schema mismatch
    loss_id = db.session.execute(text("SELECT id FROM auth_subject WHERE slug='loss'")).scalar()
    
    if loss_id:
        existing = db.session.execute(text("SELECT id FROM auth_pricing WHERE subject_id=:id"), {"id": loss_id}).scalar()
        if not existing:
            db.session.execute(text("""
                INSERT INTO auth_pricing (subject_id, role, plan, currency, amount_cents, is_active)
                VALUES (:id, 'learner', 'enrollment', 'ZAR', 10000, true)
            """), {"id": loss_id})
            db.session.commit()
            print("Successfully added R 100.00 pricing for Loss module.")
        else:
            print("Pricing already exists for Loss module.")
    else:
        print("Loss subject not found!")
