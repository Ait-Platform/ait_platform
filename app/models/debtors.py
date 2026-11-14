from sqlalchemy.types import Numeric, TypeDecorator
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
    letterhead_url = db.Column(db.String(255))
    use_custom_letterhead = db.Column(db.Boolean, default=False)
    interest_rate = db.Column(db.Float, default=2.0)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class BusinessBankAccount(db.Model):
    __tablename__ = 'business_bank_account'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    
    bank_name = db.Column(db.String(150), nullable=True)
    account_name = db.Column(db.String(150), nullable=True)
    account_number = db.Column(db.String(50), nullable=True)
    bsb_branch = db.Column(db.String(50), nullable=True)
    swift_code = db.Column(db.String(50), nullable=True)
    raw_details = db.Column(db.Text, nullable=True) # Legacy unstructured text
    
    is_default = db.Column(db.Boolean, default=False)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    debtors = db.relationship('Debtor', backref='bank_account', lazy='dynamic')

class SenderProfile(db.Model):
    __tablename__ = 'sender_profile'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    
    business_name = db.Column(db.String(255), nullable=True)
    address = db.Column(db.Text, nullable=True)
    phone = db.Column(db.String(50), nullable=True)
    email = db.Column(db.String(150), nullable=True)
    logo_url = db.Column(db.String(500), nullable=True)
    letterhead_url = db.Column(db.String(255))
    use_custom_letterhead = db.Column(db.Boolean, default=False)
    is_default = db.Column(db.Boolean, default=False)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    debtors = db.relationship('Debtor', backref='sender_profile', lazy='dynamic')

class Debtor(db.Model):
    __tablename__ = 'debtor'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    shop_id = db.Column(db.Integer, db.ForeignKey('mech_shops.id'), nullable=True)
    address = db.Column(db.Text, nullable=True)
    bank_account_id = db.Column(db.Integer, db.ForeignKey('business_bank_account.id'), nullable=True)
    sender_profile_id = db.Column(db.Integer, db.ForeignKey('sender_profile.id'), nullable=True)
    
    slug_reference = db.Column(db.String(50), nullable=True) # e.g. 'billing', 'mechanic'
    reference_id = db.Column(db.Integer, nullable=True) # e.g. tenant_id or client_id
    
    name = db.Column(db.String(255), nullable=False)
    email = db.Column(db.String(150), nullable=True)
    phone = db.Column(db.String(50), nullable=True)
    
    opening_balance = db.Column(db.Integer, default=0, server_default=db.text('0')) # In cents
    apply_interest = db.Column(db.Boolean, default=True)
    interest_rate = db.Column(db.Float, nullable=True) # Custom interest rate for this debtor (overrides profile)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)
    
    ledgers = db.relationship('DebtorLedger', backref='debtor', cascade="all, delete-orphan", lazy='dynamic')
    charge_maps = db.relationship('DebtorChargeMap', backref='debtor', cascade="all, delete-orphan")

class LedgerCents(TypeDecorator):
    """Exact integer cents with the full precision of existing BOQ quote totals."""
    impl = Numeric(28, 0)
    cache_ok = True

    def process_result_value(self, value, dialect):
        return int(value) if value is not None else None


class DebtorLedger(db.Model):
    __tablename__ = 'debtor_ledger'
    id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.Integer, db.ForeignKey('debtor.id'), nullable=False)
    
    txn_date = db.Column(db.Date, nullable=False, default=datetime.utcnow)
    description = db.Column(db.String(255), nullable=False)
    kind = db.Column(db.String(20), nullable=False) # 'debit' or 'credit'
    amount = db.Column(LedgerCents(), nullable=False) # In cents
    ref = db.Column(db.String(100), nullable=True)
    
    boq_quote_id = db.Column(db.Integer, db.ForeignKey('protrade_boq_quotes.id', name='fk_debtor_ledger_boq_quote'), nullable=True)
    payment_key = db.Column(db.String(32), nullable=True)
    __table_args__ = (
        db.UniqueConstraint('boq_quote_id', name='uq_debtor_ledger_boq_quote'),
        db.UniqueConstraint('debtor_id', 'payment_key', name='uq_debtor_payment_key'),
        db.Index('uq_debtor_payment_fingerprint', 'debtor_id', 'txn_date', 'amount', 'ref', unique=True, postgresql_where=db.text('payment_key IS NOT NULL')),
        db.CheckConstraint("boq_quote_id IS NULL OR (kind = 'debit' AND payment_key IS NULL AND amount >= 0)", name='ck_debtor_quote_debit'),
        db.CheckConstraint("payment_key IS NULL OR (kind = 'credit' AND boq_quote_id IS NULL AND amount > 0 AND ref IS NOT NULL AND length(trim(ref)) > 0)", name='ck_debtor_payment_credit'),
    )
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class DebtorChargeMap(db.Model):
    __tablename__ = 'debtor_charge_map'
    id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.Integer, db.ForeignKey('debtor.id'), nullable=False)
    
    charge_description = db.Column(db.String(255), nullable=False)
    amount = db.Column(db.Integer, nullable=False) # In cents
    frequency = db.Column(db.String(50), default='monthly') # e.g. 'monthly', 'weekly', 'once'
    day_of_month = db.Column(db.Integer, default=1) # 1-31
    start_date = db.Column(db.Date, nullable=True) # Optional start date for recurring charge
    end_date = db.Column(db.Date, nullable=True) # Optional end date for recurring charge
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
