import sys
with open("app/models/uip_governance.py", "r", encoding="utf-8") as f:
    c = f.read()

c = c.replace(
    'email = db.Column(db.String(255), nullable=False, index=True)',
    'email = db.Column(db.String(255), nullable=False, index=True)\n    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)'
)

with open("app/models/uip_governance.py", "w", encoding="utf-8") as f:
    f.write(c)
