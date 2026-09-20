with open("app/models/uip_governance.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace('display_order = db.Column(db.Integer, default=0)', 
                    'display_order = db.Column(db.Integer, default=0)\n    duty = db.Column(db.String(50), nullable=False, default="committee_member")')

with open("app/models/uip_governance.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Added duty column to model")
