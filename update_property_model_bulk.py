import re

with open('app/models/billing.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Add to BilProperty
old_fields = "    is_bulk_metered = db.Column(db.Integer, default=0)"
new_fields = "    is_bulk_metered = db.Column(db.Integer, default=0)\n    is_bulk_water = db.Column(db.Boolean, default=False)\n    is_bulk_elec = db.Column(db.Boolean, default=False)"

if old_fields in content:
    content = content.replace(old_fields, new_fields)
    with open('app/models/billing.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Updated BilProperty model with dual bulk toggles.")
else:
    print("Could not find is_bulk_metered in BilProperty.")
