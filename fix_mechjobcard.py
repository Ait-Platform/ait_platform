import re

with open('app/models/mechanic.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_fields = '''    total_amount = db.Column(db.Float, default=0.0)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)'''

new_fields = '''    total_amount = db.Column(db.Float, default=0.0)
    mileage = db.Column(db.String(50))
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)'''

content = content.replace(old_fields, new_fields)

with open('app/models/mechanic.py', 'w', encoding='utf-8') as f:
    f.write(content)
