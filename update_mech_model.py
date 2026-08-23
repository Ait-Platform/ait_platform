import re

with open('app/models/mechanic.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("terms_and_conditions = db.Column(db.Text)", "terms_and_conditions = db.Column(db.Text)\n    bank_details = db.Column(db.Text)")

with open('app/models/mechanic.py', 'w', encoding='utf-8') as f:
    f.write(content)
