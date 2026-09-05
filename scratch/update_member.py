import re

with open('app/models/core.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_member = """class CoreOrganizationMember(db.Model):
    __tablename__ = "core_organization_member"

    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    joined_at = db.Column(db.DateTime, default=datetime.utcnow)

    # Relationships"""

new_member = """class CoreOrganizationMember(db.Model):
    __tablename__ = "core_organization_member"

    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    joined_at = db.Column(db.DateTime, default=datetime.utcnow)
    left_at = db.Column(db.DateTime, nullable=True)

    # Relationships"""

text = text.replace(old_member, new_member)

with open('app/models/core.py', 'w', encoding='utf-8') as f:
    f.write(text)
