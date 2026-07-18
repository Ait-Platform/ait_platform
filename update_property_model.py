import re

with open('app/models/billing.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_fields = "    expected_bills = db.Column(db.Integer, default=1)"
new_fields = "    expected_bills = db.Column(db.Integer, default=1)\n    expected_water_meters = db.Column(db.Integer, default=0)\n    expected_elec_meters = db.Column(db.Integer, default=0)"

if old_fields in content:
    content = content.replace(old_fields, new_fields)
    with open('app/models/billing.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Updated BilProperty model.")
else:
    print("Could not find expected_bills field in BilProperty.")
