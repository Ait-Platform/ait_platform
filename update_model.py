import re

with open('app/models/mechanic.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("deposit_amount = db.Column(db.Float, default=0.0)", "deposit_amount = db.Column(db.Float, default=0.0)\n    payment_method = db.Column(db.String(50), default='EFT')")

with open('app/models/mechanic.py', 'w', encoding='utf-8') as f:
    f.write(content)
