import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix block 1
content = re.sub(
    r'        units = BilProperty\.query\.filter_by\(property_id=prop\.id\)\.all\(\)\n        for u in units:\n            meters = BilMeter\.query\.filter_by\(property_id=property\.id\)\.all\(\)',
    r'        meters = BilMeter.query.filter_by(property_id=prop.id).all()',
    content
)

# Fix block 2
content = re.sub(
    r'    units = BilProperty\.query\.filter_by\(property_id=prop\.id\)\.all\(\)\n    # Simple rendering, we can pass property and units\n    return render_template\("program_billing/view_property\.html", property=prop, account_meters=account_meters, units=units\)',
    r'    return render_template("program_billing/view_property.html", property=prop, account_meters=account_meters)',
    content
)

# Fix block 3
content = re.sub(
    r'    units = BilProperty\.query\.filter_by\(property_id=prop\.id\)\.all\(\)\n    unit = units\[0\] if units else None\n    tenant = None\n    lease = None\n    if unit:\n        tenant = BilTenant\.query\.filter_by\(property_id=property\.id\)\.first\(\)',
    r'    tenant = BilTenant.query.filter_by(property_id=prop.id).first()\n    lease = None\n    if tenant:',
    content
)

# Fix block 4 (and 6 and 8)
content = re.sub(
    r'    units = BilProperty\.query\.filter_by\(property_id=prop\.id\)\.all\(\)\n\n?    # Collect all meters attached to any unit in this property\n    all_meters = \[\]\n    for u in units:\n        all_meters\.extend\(u\.meters\)',
    r'    all_meters = prop.meters',
    content
)

content = re.sub(
    r'    units = BilProperty\.query\.filter_by\(property_id=prop\.id\)\.all\(\)\n    all_meters = \[\]\n    for u in units:\n        all_meters\.extend\(u\.meters\)',
    r'    all_meters = prop.meters',
    content
)

# Fix block 5
content = re.sub(
    r'    units = BilProperty\.query\.filter_by\(property_id=property_id\)\.all\(\)\n    all_meters = \[\]\n    for u in units:\n        all_meters\.extend\(u\.meters\)',
    r'    all_meters = prop.meters',
    content
)

# Fix block 7
content = re.sub(
    r'    # In billing we map tenants to the property via its primary sectional unit\n    unit = BilProperty\.query\.filter_by\(property_id=prop\.id\)\.first\(\)\n    if not unit:\n        # Create a default unit if it doesn\'t exist\n        unit = BilProperty\(property_id=prop\.id, name="Unit 1"\)\n        db\.session\.add\(unit\)\n        db\.session\.flush\(\)',
    r'',
    content
)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Fixes applied.")
