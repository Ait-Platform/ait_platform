import re

with open('app/models/billing.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_fields = """    pointing_to = db.Column(db.String(100), nullable=True)
    municipal_bill_number = db.Column(db.String(100), nullable=True)
    status = db.Column(db.String(50), default="active")"""

new_fields = """    pointing_to = db.Column(db.String(100), nullable=True)
    municipal_bill_number = db.Column(db.String(100), nullable=True)
    status = db.Column(db.String(50), default="active")
    date_stolen = db.Column(db.Date, nullable=True)
    date_replaced = db.Column(db.Date, nullable=True)"""

if old_fields in content:
    content = content.replace(old_fields, new_fields)
    with open('app/models/billing.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Added date_stolen and date_replaced to BilMeter model.")
else:
    print("Could not find fields to replace in models/billing.py")
