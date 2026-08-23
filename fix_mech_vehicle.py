import re

with open('app/models/mechanic.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Add license_disk_url to MechVehicle
content = content.replace("mileage = db.Column(db.Integer)", "mileage = db.Column(db.Integer)\n    license_disk_url = db.Column(db.String(500))")

with open('app/models/mechanic.py', 'w', encoding='utf-8') as f:
    f.write(content)

with open('app/__init__.py', 'r', encoding='utf-8') as f:
    content2 = f.read()

inject2 = '''        try:
            db.session.execute(text("ALTER TABLE mech_vehicles ADD COLUMN mileage INTEGER;"))
            db.session.commit()
        except Exception:
            db.session.rollback()

'''

content2 = content2.replace('        try:\n            db.session.execute(text("ALTER TABLE mech_vehicles ADD COLUMN engine_no VARCHAR(100);"))', inject2 + '        try:\n            db.session.execute(text("ALTER TABLE mech_vehicles ADD COLUMN engine_no VARCHAR(100);"))')

with open('app/__init__.py', 'w', encoding='utf-8') as f:
    f.write(content2)
