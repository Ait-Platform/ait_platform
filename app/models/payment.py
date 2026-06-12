# app/models/payment.py
from datetime import datetime
from sqlalchemy import Table
from app.extensions import db

# ---- helpers ---------------------------------------------------------------

def _try_reflect(names):
    """Return a reflected Table if any of the provided names exists; else None."""
    for name in names:
        try:
            return Table(name, db.metadata, autoload_with=db.engine)
        except Exception:
            continue
    return None

# Try to reflect common names (include your 'sprite_payment' typo just in case)
_PAYMENT_TABLE = _try_reflect(["stripe_payment", "sprite_payment", "payments", "payment"])
_SUB_TABLE     = _try_reflect(["stripe_subscription", "sprite_subscription", "subscriptions", "subscription"])

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


class Subscription(db.Model):
    """
    Stripe subscriptions. Same reflection/fallback pattern.
    """
    if _SUB_TABLE is not None:
        __table__ = _SUB_TABLE
    else:
        __tablename__ = "stripe_subscription"
        id = db.Column(db.Integer, primary_key=True)
        user_id = db.Column(db.Integer, nullable=True, index=True)

        stripe_subscription_id = db.Column(db.String(255), unique=True, index=True)
        customer_id = db.Column(db.String(255), index=True)
        price_id = db.Column(db.String(255))
        status = db.Column(db.String(50))          # trialing, active, past_due, canceled, etc.

        plan_amount = db.Column(db.Integer)        # cents
        plan_interval = db.Column(db.String(20))   # month, year
        currency = db.Column(db.String(10))

        current_period_start = db.Column(db.DateTime)
        current_period_end = db.Column(db.DateTime)
        cancel_at_period_end = db.Column(db.Boolean)
        cancel_at = db.Column(db.DateTime)
        canceled_at = db.Column(db.DateTime)
        default_payment_method = db.Column(db.String(255))
        latest_invoice_id = db.Column(db.String(255))

        created_at = db.Column(db.DateTime, default=datetime.utcnow)
        updated_at = db.Column(db.DateTime, onupdate=datetime.utcnow)

    @classmethod
    def upsert_from_subscription_obj(cls, sub: dict):
        from datetime import datetime as dt
        items = (sub.get("items") or {}).get("data") or []
        price = (items[0].get("price") if items else {}) or (sub.get("price") or {})
        vals = {
            "stripe_subscription_id": sub.get("id"),
            "customer_id": sub.get("customer"),
            "status": sub.get("status"),
            "current_period_start": dt.fromtimestamp(sub["current_period_start"]) if sub.get("current_period_start") else None,
            "current_period_end": dt.fromtimestamp(sub["current_period_end"]) if sub.get("current_period_end") else None,
            "cancel_at_period_end": sub.get("cancel_at_period_end"),
            "cancel_at": dt.fromtimestamp(sub["cancel_at"]) if sub.get("cancel_at") else None,
            "canceled_at": dt.fromtimestamp(sub["canceled_at"]) if sub.get("canceled_at") else None,
            "default_payment_method": sub.get("default_payment_method"),
            "latest_invoice_id": sub.get("latest_invoice"),
            "price_id": price.get("id"),
            "plan_amount": price.get("unit_amount"),
            "plan_interval": (price.get("recurring") or {}).get("interval"),
            "currency": price.get("currency"),
        }

        q = None
        if hasattr(cls, "stripe_subscription_id") and vals.get("stripe_subscription_id"):
            q = cls.query.filter_by(stripe_subscription_id=vals["stripe_subscription_id"]).first()

        if q:
            for k, v in vals.items():
                if hasattr(q, k):
                    setattr(q, k, v)
            return q

        filtered = {k: v for k, v in vals.items() if hasattr(cls, k)}
        obj = cls(**filtered)
        db.session.add(obj)
        return obj

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

