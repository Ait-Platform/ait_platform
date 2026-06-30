import os
import glob

search_text = "Practice CRM"
replace_text = "Medical Practice Customer Relation Management"

files_to_check = glob.glob('templates/program_practice_crm/*.html') + ['templates/public/receptionist_register.html', 'templates/public/welcome.html', 'app/program_practice_crm/routes.py']

for filepath in files_to_check:
    if not os.path.exists(filepath): continue
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    if search_text in content:
        new_content = content.replace(search_text, replace_text)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"Updated {filepath}")

# Update the database
from app import create_app
from app.extensions import db
from app.models.auth import AuthSubject

app = create_app()
with app.app_context():
    subject = AuthSubject.query.filter_by(slug='practice').first()
    if subject and subject.name == search_text:
        subject.name = replace_text
        db.session.commit()
        print("Updated auth_subject in database.")
