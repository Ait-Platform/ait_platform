# app/payments/quote.py
from datetime import datetime

from flask import current_app
from app.extensions import db
from app.models.auth import UserEnrollment
from app.payments.pricing import price_cents_for
from decimal import Decimal
from sqlalchemy import text

def detect_country(request) -> str:
    cc = (request.headers.get("CF-IPCountry") or "").strip().upper()
    if len(cc) == 2:
        return cc
    return (request.form.get("country") or "ZA").strip().upper()

def lock_enrollment_quote(enrollment_id: int, subject_slug: str, request, price_version="2025-11"):
    country = detect_country(request)
    # With PayFast you’ll likely always use ZAR here
    currency = "ZAR"
    amount_cents = price_cents_for(subject_slug, currency) or 5000

    ue = UserEnrollment.query.get(enrollment_id)
    if not ue:
        return

    ue.country_code = country
    ue.quoted_currency = currency
    ue.quoted_amount_cents = amount_cents
    ue.price_version = price_version
    ue.price_locked_at = datetime.utcnow()
    db.session.commit()

def fx_for_country_code(code: str) -> Decimal:
    """
    Look up FX (1 local = fx_to_zar ZAR). If the column/table
    is missing (e.g. old SQLite schema), fall back to 1.0.
    """
    try:
        row = db.session.execute(
            text("""
                SELECT fx_to_zar
                FROM ref_country_currency
                WHERE alpha2 = :cc
                LIMIT 1
            """),
            {"cc": code},
        ).first()
    except Exception as exc:
        current_app.logger.warning(
            "fx_for_country_code fallback for %s: %s", code, exc
        )
        return Decimal("1.0")

    if not row:
        return Decimal("1.0")

    val = getattr(row, "fx_to_zar", None)
    if val is None:
        return Decimal("1.0")

    return Decimal(str(val))
