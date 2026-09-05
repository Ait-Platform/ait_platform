import re
with open('app/models/core.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Check if CoreOrganizationWallet exists
if 'CoreOrganizationWallet' not in text:
    old_org = """class CoreOrganization(db.Model):
    __tablename__ = "core_organization"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)"""

    new_org = """class CoreOrganizationWallet(db.Model):
    __tablename__ = "core_organization_wallet"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey('core_organization.id'), nullable=False, unique=True)
    balance = db.Column(db.Integer, default=1000, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    transactions = db.relationship("CoreOrganizationLedger", back_populates="wallet", cascade="all, delete-orphan")

class CoreOrganizationLedger(db.Model):
    __tablename__ = "core_organization_ledger"
    id = db.Column(db.Integer, primary_key=True)
    wallet_id = db.Column(db.Integer, db.ForeignKey('core_organization_wallet.id'), nullable=False)
    amount = db.Column(db.Integer, nullable=False)
    description = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    wallet = db.relationship("CoreOrganizationWallet", back_populates="transactions")

class CoreOrganization(db.Model):
    __tablename__ = "core_organization"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)"""
    
    text = text.replace(old_org, new_org)
    
    # Also add the wallet relationship to CoreOrganization
    text = text.replace('members = db.relationship("CoreOrganizationMember", back_populates="organization", cascade="all, delete-orphan")', 'members = db.relationship("CoreOrganizationMember", back_populates="organization", cascade="all, delete-orphan")\n    wallet = db.relationship("CoreOrganizationWallet", uselist=False, backref="organization")')
    
    with open('app/models/core.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Added wallet and ledger models.")
else:
    print("Models already exist.")
