# app/models/payment.py
from datetime import datetime
from app.extensions import db

class YocoPayment(db.Model):
    __tablename__ = "yoco_payment"
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True, index=True)
    email = db.Column(db.String(255), index=True)
    subject_slug = db.Column(db.String(120), index=True)
    
    amount_cents = db.Column(db.Integer, nullable=False)           # cents
    currency = db.Column(db.String(10), default="ZAR")
    status = db.Column(db.String(50), default="pending")           # 'pending', 'completed', 'canceled', etc.
    
    checkout_id = db.Column(db.String(255), unique=True, index=True)
    gateway_reference = db.Column(db.String(255))
    
    paid_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, onupdate=datetime.utcnow)

class RefCountryCurrency(db.Model):
    __tablename__ = "ref_country_currency"
    alpha2 = db.Column(db.String(2), primary_key=True)
    currency = db.Column(db.String(3), nullable=False)
    updated_at = db.Column(db.DateTime, nullable=False, server_default=db.func.current_timestamp())
    name = db.Column(db.Text)
    fx_to_zar = db.Column(db.Numeric(18, 6))
    is_active = db.Column(db.Boolean, nullable=False, default=True)

class SubjectCountryPrice(db.Model):
    __tablename__ = "subject_country_price"

    id = db.Column(db.Integer, primary_key=True)
    subject_id = db.Column(db.Integer, db.ForeignKey("auth_subject.id"), nullable=False)
    country_code = db.Column(db.Text, nullable=False)

    local_amount_cents = db.Column(db.Integer, nullable=False)
    zar_amount_cents = db.Column(db.Integer, nullable=False)

    local_currency = db.Column(db.Text, nullable=False)
    is_active = db.Column(db.Boolean, nullable=False, default=True)

    created_at = db.Column(db.DateTime, server_default=db.func.current_timestamp())

    price_version = db.Column(db.Integer, nullable=False, default=1)

    __table_args__ = (
        db.Index("idx_subject_country", "subject_id", "country_code"),
    )

    @property
    def local_amount(self):
        return self.local_amount_cents / 100 if self.local_amount_cents else None

    @property
    def zar_amount(self):
        return self.zar_amount_cents / 100 if self.zar_amount_cents else None

