with open("app/models/uip.py", "r", encoding="utf-8") as f:
    text = f.read()

old_fields = """    email = db.Column(db.String(255))
    phone = db.Column(db.String(50))
    is_active = db.Column(db.Boolean, nullable=False, default=True)"""

new_fields = """    email = db.Column(db.String(255))
    phone = db.Column(db.String(50))
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    
    # Onboarding Campaign Tracking
    invite_wave = db.Column(db.Integer, nullable=False, default=0)
    last_invite_at = db.Column(db.DateTime, nullable=True)"""

text = text.replace(old_fields, new_fields)

with open("app/models/uip.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated uip_member_profile schema")
