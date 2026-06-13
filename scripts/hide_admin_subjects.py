import os
import sys

# Ensure the app package is accessible
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

def hide_admin_subjects():
    app = create_app()
    with app.app_context():
        # Find all subjects that are program_type = 'admin'
        admin_subjects = AuthSubject.query.filter_by(program_type="admin").all()
        
        count = 0
        for subj in admin_subjects:
            if not subj.is_hidden_on_bridge:
                subj.is_hidden_on_bridge = True
                print(f"Hiding '{subj.name}' (slug: {subj.slug}) from the Bridge Dashboard.")
                count += 1
                
        if count > 0:
            db.session.commit()
            print(f"Successfully hid {count} admin subject(s) from the Bridge.")
        else:
            print("All admin subjects are already hidden. No changes made.")

if __name__ == "__main__":
    hide_admin_subjects()
