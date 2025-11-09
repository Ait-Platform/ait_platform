# app/models/stripe_payment.py
from app.extensions import db
from sqlalchemy import func

class StripePayment(db.Model):
    __tablename__ = "stripe_payment"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer)
    stripe_session_id = db.Column(db.String(255), index=True, unique=False)
    stripe_payment_intent_id = db.Column(db.String(255), index=True, unique=True)
    customer_id = db.Column(db.String(255))
    email = db.Column(db.String(255))
    amount_total = db.Column(db.Integer)           # cents
    currency = db.Column(db.String(10))
    status = db.Column(db.String(50))
    purpose = db.Column(db.String(120))
    next_url = db.Column(db.Text)
    receipt_url = db.Column(db.Text)
    paid_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, server_default=func.now())
    updated_at = db.Column(db.DateTime, server_default=func.now(), onupdate=func.now())
