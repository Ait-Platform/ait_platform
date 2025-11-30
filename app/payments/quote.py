# app/payments/quote.py
from datetime import datetime
from flask import current_app
from app.extensions import db
from app.models.auth import UserEnrollment
from app.payments.pricing import get_parity_anchor_cents, price_cents_for, price_for_country
from decimal import Decimal
from sqlalchemy import text
from decimal import Decimal, ROUND_HALF_UP

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

def fx_for_country_code(code: str) -> Decimal | None:
    """
    Returns fx_to_zar if available.
    Returns None if:
      - row missing
      - fx_to_zar NULL or <= 0
      - table errors
    """
    try:
        row = db.session.execute(
            text("""
                SELECT fx_to_zar
                FROM ref_country_currency
                WHERE alpha2 = :cc AND is_active = true
                LIMIT 1
            """),
            {"cc": code},
        ).first()
    except Exception as exc:
        current_app.logger.warning(
            "fx_for_country_code error for %s: %s", code, exc
        )
        return None

    if not row or row.fx_to_zar is None:
        return None

    fx = Decimal(str(row.fx_to_zar))
    return fx if fx > 0 else None



DISCOUNT_RATE = Decimal("0.10")  # 10% off on ZAR anchor


def build_amount_quote(subject, country_code: str, discounted: bool) -> dict:
    """
    Single source of truth for:
    - base ZAR anchor (subject-level value)
    - optional 10% discount on ZAR
    - parity price for UI (local currency)
    - PayFast amount in ZAR as '123.45'
    """

    # 1) Get your anchor in ZAR cents (reuse your existing logic)
    anchor_zar_cents = int(get_parity_anchor_cents(subject) or 0)

    # 2) Apply 10% discount ON THE ZAR VALUE if needed
    if discounted:
        final_zar = (
            Decimal(anchor_zar_cents)
            * (Decimal("1.0") - DISCOUNT_RATE)
        )
        final_zar_cents = int(final_zar.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        is_discount = True
    else:
        final_zar_cents = anchor_zar_cents
        is_discount = False

    # 3) Parity pricing for the user’s country (you already have price_for_country)
    local_cents, local_currency = price_for_country(country_code, final_zar_cents)

    # 4) PayFast wants ZAR as rands with 2 decimals, e.g. "123.45"
    final_zar_rands = (
        Decimal(final_zar_cents) / Decimal("100")
    ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    final_zar_str = f"{final_zar_rands:.2f}"

    return {
        "anchor_zar_cents": anchor_zar_cents,   # original value in ZAR cents
        "final_zar_cents": final_zar_cents,     # after discount (or same)
        "final_zar_str": final_zar_str,         # for PayFast "amount"
        "local_cents": local_cents,             # for UI
        "local_currency": local_currency,       # "ZAR", "USD", etc
        "is_discount": is_discount,             # banner text / copy
    }
