import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject
from sqlalchemy import text

def test():
    app = create_app()
    with app.app_context():
        print("Testing AuthSubject database state...")
        
        subjects = db.session.execute(text("""
            SELECT slug, is_hidden_on_bridge, parent_subject_id, bypass_dashboard_endpoint, start_endpoint
            FROM auth_subject
            WHERE slug IN ('home_premium', 'home2', 'cfi_judge', 'cultural_fire', 'sms', 'adv_math', 'loss', 'home')
        """)).mappings().all()
        
        for s in subjects:
            print(f"Slug: {s['slug']}")
            print(f"  Hidden: {s['is_hidden_on_bridge']}")
            print(f"  Parent: {s['parent_subject_id']}")
            print(f"  Bypass: {s['bypass_dashboard_endpoint']}")
            print(f"  Start:  {s['start_endpoint']}")
            print("-" * 40)

if __name__ == "__main__":
    test()
