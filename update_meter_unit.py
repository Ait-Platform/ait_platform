with open('app/models/billing.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_col = "sectional_unit_id = db.Column(db.Integer, db.ForeignKey('bil_sectional_unit.id'), nullable=False)"
new_col = "sectional_unit_id = db.Column(db.Integer, db.ForeignKey('bil_sectional_unit.id'), nullable=True)"

if old_col in content:
    content = content.replace(old_col, new_col)
    with open('app/models/billing.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Updated sectional_unit_id to nullable=True in BilMeter.")
else:
    print("Could not find the column definition to replace.")
