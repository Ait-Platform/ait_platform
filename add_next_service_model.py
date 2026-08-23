import re

with open('app/models/mechanic.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''    deposit_amount = db.Column(db.Float, default=0.0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)''',
    '''    deposit_amount = db.Column(db.Float, default=0.0)
    next_service_due = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)'''
)

with open('app/models/mechanic.py', 'w', encoding='utf-8') as f:
    f.write(content)
