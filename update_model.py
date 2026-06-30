with open('app/models/billing.py', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''    # ?? Link to the manager who owns this property
    manager_id = db.Column('''

injection = '''    # ?? Onboarding State
    onboarding_status = db.Column(db.String(50), default='active')
    expected_bills = db.Column(db.Integer, default=0)
    expected_tenants = db.Column(db.Integer, default=0)
    is_bulk_metered = db.Column(db.Integer, default=0)
    expected_sub_meters = db.Column(db.Integer, default=0)

    # ?? Link to the manager who owns this property
    manager_id = db.Column('''

content = content.replace(target, injection)

with open('app/models/billing.py', 'w', encoding='utf-8') as f:
    f.write(content)
