# app/utils/pricing.py
from sqlalchemy import text
from app.extensions import db
from flask import current_app
from decimal import Decimal
from typing import Optional, Union
from datetime import datetime
from sqlalchemy import and_, or_, func

from app.models.auth import AuthSubject, AuthPricing



DEFAULT_CURRENCY = "ZAR"

def get_currency() -> str:
    return (current_app.config.get("STRIPE_CURRENCY") or DEFAULT_CURRENCY).upper()


# Change this to your real table name:
TABLE = "subject_pricing"


def _resolve_subject_id(subject) -> Optional[int]:

    if isinstance(subject, int):
        return subject
    if isinstance(subject, str):
        slug = subject.strip().lower()
        row = (db.session.query(AuthSubject.id)
               .filter(func.lower(func.coalesce(AuthSubject.slug, AuthSubject.name)) == slug)
               .first())
        return row[0] if row else None
    if hasattr(subject, "id"):
        return int(getattr(subject, "id"))
    if hasattr(subject, "slug") and getattr(subject, "slug"):
        return _resolve_subject_id(getattr(subject, "slug"))
    if hasattr(subject, "name") and getattr(subject, "name"):
        return _resolve_subject_id(getattr(subject, "name"))
    return None

def format_currency(amount: Decimal, currency: str = "ZAR") -> str:
    return f"{currency} {amount:.2f}"


def _plan_row(subject_slug: str, role: str, plan: str):
    params = {
        "subj": (subject_slug or "").strip().lower(),
        "role": (role or "").strip().lower(),
        "plan": (plan or "enrollment").strip().lower(),
    }

    # 1) exact role
    row = db.session.execute(text("""
        SELECT ap.currency, ap.amount_cents
        FROM auth_pricing ap
        JOIN auth_subject s ON s.id = ap.subject_id
        WHERE lower(COALESCE(s.slug, s.name)) = :subj
          AND lower(COALESCE(ap.role, '')) = :role
          AND lower(ap.plan) = :plan
          AND ap.is_active IN (1, '1')
          AND ap.active_from <= CURRENT_TIMESTAMP
          AND (ap.active_to IS NULL OR ap.active_to >= CURRENT_TIMESTAMP)
        ORDER BY ap.active_from DESC
        LIMIT 1
    """), params).mappings().first()

    if row:
        return row

    # 2) role-agnostic (NULL role)
    row = db.session.execute(text("""
        SELECT ap.currency, ap.amount_cents
        FROM auth_pricing ap
        JOIN auth_subject s ON s.id = ap.subject_id
        WHERE lower(COALESCE(s.slug, s.name)) = :subj
          AND ap.role IS NULL
          AND lower(ap.plan) = :plan
          AND ap.is_active IN (1, '1')
          AND ap.active_from <= CURRENT_TIMESTAMP
          AND (ap.active_to IS NULL OR ap.active_to >= CURRENT_TIMESTAMP)
        ORDER BY ap.active_from DESC
        LIMIT 1
    """), params).mappings().first()

    return row

def get_subject_plan(subject_slug: str, role: str, plan: str = "enrollment"):
    row = _plan_row(subject_slug, role, plan)
    if not row:
        return None
    amount_major = (Decimal(row["amount_cents"] or 0) / Decimal(100)).quantize(Decimal("0.01"))
    return {"currency": row["currency"], "amount": amount_major}

def get_subject_price(subject_slug: str, role: str, plan: str = "enrollment", currency: str | None = None):
    # convenience wrapper if you need just the Decimal
    res = get_subject_plan(subject_slug, role, plan)
    return res and res["amount"]

# app/utils/pricing.py
from decimal import Decimal

MIN_BY_CCY = {"ZAR": Decimal("9.00"), "USD": Decimal("0.50")}
def clamp_min(amount: Decimal, currency: str) -> Decimal:
    return max(amount, MIN_BY_CCY.get(currency.upper(), Decimal("0.50")))
