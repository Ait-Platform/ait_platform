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

# ---- Payment ---------------------------------------------------------------

class Payment(db.Model):
    """
    Stripe one-off payments. If an existing table is found, we bind to it.
    Otherwise we define an explicit schema on 'stripe_payment'.
    """
    if _PAYMENT_TABLE is not None:
        __table__ = _PAYMENT_TABLE
    else:
        __tablename__ = "stripe_payment"
        id = db.Column(db.Integer, primary_key=True)
        user_id = db.Column(db.Integer, nullable=True, index=True)

        stripe_session_id = db.Column(db.String(255), unique=True, index=True)
        stripe_payment_intent_id = db.Column(db.String(255), index=True)
        customer_id = db.Column(db.String(255), index=True)
        email = db.Column(db.String(255))

        amount_total = db.Column(db.Integer)           # cents
        currency = db.Column(db.String(10))            # 'zar'
        status = db.Column(db.String(50))              # 'paid','unpaid','canceled', etc.
        purpose = db.Column(db.String(120))            # e.g. 'loss_enrollment'
        next_url = db.Column(db.Text)                  # optional redirect
        receipt_url = db.Column(db.Text)

        paid_at = db.Column(db.DateTime, nullable=True)
        created_at = db.Column(db.DateTime, default=datetime.utcnow)
        updated_at = db.Column(db.DateTime, onupdate=datetime.utcnow)

    # ---- upsert from Stripe checkout.session --------------------------------
    @classmethod
    def upsert_from_checkout_session(cls, sess: dict):
        """
        Idempotently create/update from Stripe 'checkout.session.completed' object.
        """
        meta = (sess.get("metadata") or {})
        cust = (sess.get("customer_details") or {})
        vals = {
            "stripe_session_id": sess.get("id"),
            "stripe_payment_intent_id": sess.get("payment_intent"),
            "customer_id": sess.get("customer"),
            "email": cust.get("email") or sess.get("customer_email"),
            "amount_total": sess.get("amount_total"),
            "currency": sess.get("currency"),
            "status": sess.get("payment_status"),  # 'paid' on success
            "purpose": meta.get("purpose"),
            "next_url": meta.get("next"),
            "user_id": meta.get("user_id"),
            "paid_at": datetime.utcnow() if sess.get("payment_status") == "paid" else None,
        }

        # find by session_id first; else by payment_intent
        q = None
        if hasattr(cls, "stripe_session_id") and vals.get("stripe_session_id"):
            q = cls.query.filter_by(stripe_session_id=vals["stripe_session_id"]).first()
        if not q and hasattr(cls, "stripe_payment_intent_id") and vals.get("stripe_payment_intent_id"):
            q = cls.query.filter_by(stripe_payment_intent_id=vals["stripe_payment_intent_id"]).first()

        if q:
            for k, v in vals.items():
                if hasattr(q, k):
                    setattr(q, k, v)
            return q

        # only pass columns that exist on this table
        filtered = {k: v for k, v in vals.items() if hasattr(cls, k)}
        obj = cls(**filtered)
        db.session.add(obj)
        return obj


# ---- Subscription ----------------------------------------------------------

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

# app/models/auth_pricing.py
from datetime import datetime
from app.extensions import db

class AuthPricing(db.Model):
    __tablename__ = "auth_pricing"

    id = db.Column(db.Integer, primary_key=True)
    subject_id = db.Column(db.Integer, db.ForeignKey("auth_subject.id", ondelete="CASCADE"), nullable=False)
    role = db.Column(db.String, nullable=True)             # None = any role
    plan = db.Column(db.String, nullable=False, default="enrollment")
    currency = db.Column(db.String, nullable=False, default="ZAR")
    amount_cents = db.Column(db.Integer, nullable=False, default=0)
    is_active = db.Column(db.Integer, nullable=False, default=1)  # 1/0
    active_from = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    active_to = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    # optional:
    # subject = db.relationship("AuthSubject", backref=db.backref("pricing", cascade="all, delete-orphan"))
