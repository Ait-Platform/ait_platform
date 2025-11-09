# app/payments/helpers.py
from __future__ import annotations

from sqlalchemy import text
from app.extensions import db

def mark_enrollment_paid(*, user_id: int, subject_id: int, program: str | None = None) -> None:
    """
    Ensure an ACTIVE enrollment for exactly this (user_id, subject_id).
    Uses only the columns we know exist: user_id, subject_id, status.
    NO COMMIT here; caller should commit once after all work.
    Requires UNIQUE(user_id, subject_id) on user_enrollment.
    """
    db.session.execute(text("""
        INSERT INTO user_enrollment (user_id, subject_id, status)
        VALUES (:uid, :sid, 'active')
        ON CONFLICT(user_id, subject_id) DO UPDATE SET
            status = 'active'
    """), {"uid": int(user_id), "sid": int(subject_id)})

def record_stripe_payment(
    *,
    user_id: int | None,
    session_id: str,                      # Stripe Checkout Session ID
    payment_intent_id: str | None = None, # Stripe PaymentIntent ID
    customer_id: str | None = None,
    email: str | None = None,
    amount_total: int | None = None,      # cents (INT)
    currency: str | None = None,          # 'ZAR', 'USD', ...
    status: str,                          # 'pending'|'succeeded'|'failed'|'canceled'
    purpose: str | None = None,
    next_url: str | None = None,
    receipt_url: str | None = None,
    paid_at: str | None = None,           # 'YYYY-MM-DD HH:MM:SS' UTC or None
) -> None:
    """
    Idempotent upsert by stripe_session_id into stripe_payment.
    NO COMMIT here; caller should commit.
    Make sure you have: UNIQUE(stripe_session_id) on stripe_payment.
    """
    params = {
        "user_id": user_id,
        "sid": session_id,
        "pid": payment_intent_id,
        "cid": customer_id,
        "email": (email or "").lower() or None,
        "amt": int(amount_total or 0),
        "ccy": (currency or "ZAR").upper(),
        "st": status,
        "purpose": purpose,
        "next_url": next_url,
        "receipt_url": receipt_url,
        "paid_at": paid_at,
    }

    db.session.execute(text("""
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
            paid_at
        ) VALUES (
            :user_id, :sid, :pid, :cid, :email, :amt, :ccy, :st, :purpose, :next_url, :receipt_url, :paid_at
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
            paid_at                  = COALESCE(excluded.paid_at, stripe_payment.paid_at)
    """), params)
