import re

with open('app/models/mechanic.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_fields = '''    terms_and_conditions = db.Column(db.Text)
    onboarding_status = db.Column(db.String(50), default='draft_setup') # draft_setup, draft_review, active
    created_at = db.Column(db.DateTime, default=datetime.utcnow)'''

new_fields = '''    terms_and_conditions = db.Column(db.Text)
    onboarding_status = db.Column(db.String(50), default='draft_setup') # draft_setup, draft_review, active
    shadow_spent_cents = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)'''

content = content.replace(old_fields, new_fields)

with open('app/models/mechanic.py', 'w', encoding='utf-8') as f:
    f.write(content)
