import re

with open('app/__init__.py', 'r', encoding='utf-8') as f:
    content = f.read()

patch = '''        try:
            db.session.execute(text("ALTER TABLE mech_job_cards ADD COLUMN mileage VARCHAR(50);"))
            db.session.commit()
        except Exception:
            db.session.rollback()
'''

content = content.replace('db.create_all()', 'db.create_all()\n' + patch, 1)

with open('app/__init__.py', 'w', encoding='utf-8') as f:
    f.write(content)
