import os
import sys

# Ensure the app package is accessible
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

def update_subjects():
    app = create_app()
    with app.app_context():
        # 1. Hide CFI Judge, Home Premium, Home Section 3, and Home 2 from the bridge
        slugs_to_hide = ['cfi_judge', 'home_premium', 'home_section3', 'home2']
        
        for slug in slugs_to_hide:
            subj = AuthSubject.query.filter_by(slug=slug).first()
            if subj:
                subj.is_hidden_on_bridge = True
                print(f"Set is_hidden_on_bridge = True for {slug}")
        
        # 2. Make SMS (School Management System) an admin program so it is completely hidden from normal users
        sms = AuthSubject.query.filter_by(slug='sms').first()
        if sms:
            sms.program_type = 'admin'
            sms.is_hidden_on_bridge = True
            print("Set program_type = 'admin' and is_hidden_on_bridge = True for sms")
            
        db.session.commit()
        print("Successfully updated database records.")

if __name__ == "__main__":
    update_subjects()
