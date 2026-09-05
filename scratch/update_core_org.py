import re

with open('app/models/core.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_org = """class CoreOrganization(db.Model):
    __tablename__ = "core_organization"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    slug = db.Column(db.String(255), unique=True, nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)"""

new_org = """class CoreOrganization(db.Model):
    __tablename__ = "core_organization"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    slug = db.Column(db.String(255), unique=True, nullable=False, index=True)
    
    # Extended Functional Specification Fields
    area = db.Column(db.String(255))
    municipality_ref = db.Column(db.String(255))
    contact_email = db.Column(db.String(255))
    contact_phone = db.Column(db.String(50))
    status = db.Column(db.String(50), default="active", index=True)
    config_json = db.Column(db.Text) # Storing JSON configurations as Text
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)"""

if old_org in text:
    text = text.replace(old_org, new_org)
    with open('app/models/core.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Updated CoreOrganization with Functional Spec fields.")
else:
    print("Could not find the CoreOrganization block exactly. Make sure it hasn't changed.")
