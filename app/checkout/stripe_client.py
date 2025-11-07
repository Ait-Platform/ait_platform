# app/checkout/stripe_client.py
import stripe
from flask import current_app
from app.extensions import db
from sqlalchemy import text as sa_text
import datetime as dt
from sqlalchemy.exc import OperationalError

def _stripe():
    key = current_app.config.get("STRIPE_SECRET_KEY")
    if not key:
        return None
    stripe.api_key = key
    return stripe


def record_stripe_payment(
    user_id: int | None,
    session_id: str,
    payment_intent_id: str | None,
    customer_id: str | None,
    email: str | None,
    amount_total: int | None,
    currency: str | None,
    status: str,                     # 'initiated' → later 'succeeded'
    purpose: str | None,
    next_url: str | None,
    receipt_url: str | None,
    paid_at: str | None,
):
    """Insert or update one row per Stripe session into stripe_payment."""
    email_norm = (email or "").lower() or None
    now_iso = dt.datetime.utcnow().isoformat(timespec="seconds")

    sql = sa_text("""
        INSERT INTO stripe_payment (
            user_id,
            stripe_session_id,
            stripe_payment_intent_id,
            customer_id,
            email,
            amount_total,
            currency,
            status,
            purpose,
            next_url,
            receipt_url,
            paid_at,
            created_at,
            updated_at
        )
        VALUES (
            :user_id,
            :session_id,
            :payment_intent_id,
            :customer_id,
            :email,
            :amount_total,
            :currency,
            :status,
            :purpose,
            :next_url,
            :receipt_url,
            :paid_at,
            :created_at,
            :updated_at
        )
        ON CONFLICT(stripe_session_id) DO UPDATE SET
            user_id                  = COALESCE(excluded.user_id, stripe_payment.user_id),
            stripe_payment_intent_id = COALESCE(excluded.stripe_payment_intent_id, stripe_payment.stripe_payment_intent_id),
            customer_id              = COALESCE(excluded.customer_id, stripe_payment.customer_id),
            email                    = COALESCE(excluded.email, stripe_payment.email),
            amount_total             = COALESCE(excluded.amount_total, stripe_payment.amount_total),
            currency                 = COALESCE(excluded.currency, stripe_payment.currency),
            status                   = excluded.status,
            purpose                  = COALESCE(excluded.purpose, stripe_payment.purpose),
            next_url                 = COALESCE(excluded.next_url, stripe_payment.next_url),
            receipt_url              = COALESCE(excluded.receipt_url, stripe_payment.receipt_url),
            paid_at                  = COALESCE(excluded.paid_at, stripe_payment.paid_at),
            updated_at               = excluded.updated_at
    """)

    db.session.execute(sql, {
        "user_id": user_id,
        "session_id": session_id,
        "payment_intent_id": payment_intent_id,
        "customer_id": customer_id,
        "email": email_norm,
        "amount_total": amount_total,
        "currency": (currency or "ZAR").upper(),
        "status": status,
        "purpose": purpose or "checkout",
        "next_url": next_url,
        "receipt_url": receipt_url,
        "paid_at": paid_at,
        "created_at": now_iso,
        "updated_at": now_iso,
    })


# Try these in order; add your actual name if you know it
_PRICE_TABLE_CANDIDATES = [
    "auth_pricing",
]

def _first_existing_table(candidates):
    try:
        rows = db.session.execute(sa_text(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )).all()
        existing = {r[0] for r in rows}
        for t in candidates:
            if t in existing:
                return t
    except Exception:
        pass
    return None


def fetch_subject_price(subject_slug: str, role: str = "user"):
    """
    Returns (amount_cents:int|None, currency:str|None) for the subject+role
    from auth_pricing. Role can be NULL (means any role).
    Picks the best row: exact role first, else NULL role; newest active_from wins.
    """
    slug = (subject_slug or "").strip().lower()
    r    = (role or "user").strip().lower()

    row = db.session.execute(sa_text("""
        SELECT p.amount_cents, p.currency
        FROM auth_pricing p
        JOIN auth_subject s ON s.id = p.subject_id
        WHERE lower(s.slug) = :slug
          AND p.plan = 'enrollment'
          AND COALESCE(p.is_active, 1) = 1
          AND (p.role IS NULL OR lower(p.role) = :role)
          AND (p.active_to IS NULL OR p.active_to > CURRENT_TIMESTAMP)
        ORDER BY
          CASE WHEN p.role IS NULL THEN 1 ELSE 0 END,   -- exact role first
          p.active_from DESC
        LIMIT 1
    """), {"slug": slug, "role": r}).first()

    if not row:
        return None, None

    amt = int(row[0]) if row[0] is not None else None
    cur = (row[1] or "ZAR").upper()
    return amt, cur
