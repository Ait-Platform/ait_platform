import re

with open('app/models/mechanic.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Remove unique=True from vin
content = content.replace("vin = db.Column(db.String(50), unique=True)", "vin = db.Column(db.String(50))")

with open('app/models/mechanic.py', 'w', encoding='utf-8') as f:
    f.write(content)

with open('app/__init__.py', 'r', encoding='utf-8') as f:
    content2 = f.read()

inject2 = '''        try:
            db.session.execute(text("ALTER TABLE mech_vehicles DROP CONSTRAINT IF EXISTS mech_vehicles_vin_key;"))
            db.session.commit()
        except Exception:
            db.session.rollback()

'''

content2 = content2.replace('        try:\n            db.session.execute(text("ALTER TABLE mech_vehicles ADD COLUMN mileage INTEGER;"))', inject2 + '        try:\n            db.session.execute(text("ALTER TABLE mech_vehicles ADD COLUMN mileage INTEGER;"))')

with open('app/__init__.py', 'w', encoding='utf-8') as f:
    f.write(content2)
