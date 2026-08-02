from app.extensions import db
from datetime import datetime

class SoaProfile(db.Model):
    __tablename__ = 'soa_profile'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    
    business_name = db.Column(db.String(255), nullable=True)
    address = db.Column(db.Text, nullable=True)
    phone = db.Column(db.String(50), nullable=True)
    email = db.Column(db.String(150), nullable=True)
    bank_details = db.Column(db.Text, nullable=True)
    logo_url = db.Column(db.String(500), nullable=True)
    interest_rate = db.Column(db.Float, default=2.0)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Debtor(db.Model):
    __tablename__ = 'debtor'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    
    slug_reference = db.Column(db.String(50), nullable=True) # e.g. 'billing', 'mechanic'
    reference_id = db.Column(db.Integer, nullable=True) # e.g. tenant_id or client_id
    
    name = db.Column(db.String(255), nullable=False)
    email = db.Column(db.String(150), nullable=True)
    phone = db.Column(db.String(50), nullable=True)
    
    opening_balance = db.Column(db.Integer, default=0) # In cents
    apply_interest = db.Column(db.Boolean, default=True)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)
    
    ledgers = db.relationship('DebtorLedger', backref='debtor', cascade="all, delete-orphan", lazy='dynamic')
    charge_maps = db.relationship('DebtorChargeMap', backref='debtor', cascade="all, delete-orphan")

class DebtorLedger(db.Model):
    __tablename__ = 'debtor_ledger'
    id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.Integer, db.ForeignKey('debtor.id'), nullable=False)
    
    txn_date = db.Column(db.Date, nullable=False, default=datetime.utcnow)
    description = db.Column(db.String(255), nullable=False)
    kind = db.Column(db.String(20), nullable=False) # 'debit' or 'credit'
    amount = db.Column(db.Integer, nullable=False) # In cents
    ref = db.Column(db.String(100), nullable=True)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class DebtorChargeMap(db.Model):
    __tablename__ = 'debtor_charge_map'
    id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.Integer, db.ForeignKey('debtor.id'), nullable=False)
    
    charge_description = db.Column(db.String(255), nullable=False)
    amount = db.Column(db.Integer, nullable=False) # In cents
    frequency = db.Column(db.String(50), default='monthly') # e.g. 'monthly', 'weekly', 'once'
    day_of_month = db.Column(db.Integer, default=1) # 1-31
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
