# payments/AuthPricing.py
from datetime import datetime
from sqlalchemy import select, and_, or_, func
from app.extensions import db
from flask import g, request
from datetime import datetime, timezone
from app.models.auth import AuthSubject, AuthPricing

VAT_RATE = 0.15  # SA VAT

def get_subject_price(subject_slug: str, role: str = "learner", plan: str = "enrollment"):
    """Return the active price row for a subject slug, with VAT-inclusive display."""
    now = datetime.utcnow()

    subj = db.session.execute(
        select(AuthSubject.id).where(AuthSubject.slug == subject_slug)
    ).scalar_one_or_none()
    if not subj:
        return None  # unknown subject

    row = db.session.execute(
        select(AuthPricing)
        .where(
            and_(
                AuthPricing.subject_id == subj,
                AuthPricing.role == role,
                AuthPricing.plan == plan,
                AuthPricing.is_active == True,
                or_(AuthPricing.active_from == None, AuthPricing.active_from <= now),
                or_(AuthPricing.active_to == None, AuthPricing.active_to > now),
            )
        )
        .order_by(AuthPricing.active_from.desc().nulls_last())
        .limit(1)
    ).scalar_one_or_none()

    if not row:
        return None

    # build a tiny view model
    amount = (row.amount_cents or 0) / 100.0
    total = round(amount * (1 + VAT_RATE), 2)
    currency = (row.currency or "ZAR").upper()
    # simple ZAR display
    display = f"R {total:,.2f}" if currency == "ZAR" else f"{currency} {total:,.2f}"

    return {
        "subject_id": row.subject_id,
        "currency": currency,
        "amount_cents": row.amount_cents,
        "amount_ex_vat": amount,
        "amount_incl_vat": total,
        "display": display,
        "vat_rate": VAT_RATE,
    }


# app/utils/text_format.py
def _chunk_to_words(n: int) -> str:
    ones = ["zero","one","two","three","four","five","six","seven","eight","nine",
            "ten","eleven","twelve","thirteen","fourteen","fifteen","sixteen","seventeen","eighteen","nineteen"]
    tens = ["","", "twenty","thirty","forty","fifty","sixty","seventy","eighty","ninety"]

    if n < 20:
        return ones[n]
    if n < 100:
        t, r = divmod(n, 10)
        return tens[t] + (f"-{ones[r]}" if r else "")
    h, r = divmod(n, 100)
    return ones[h] + " hundred" + (f" and {_chunk_to_words(r)}" if r else "")

def number_to_words(n: int) -> str:
    if n == 0:
        return "Zero"
    if n < 0:
        return "Minus " + number_to_words(-n)

    words = []
    thousands, rem = divmod(n, 1000)
    if thousands:
        words.append(_chunk_to_words(thousands) + " thousand")
    if rem:
        words.append(_chunk_to_words(rem))
    s = " ".join(words)
    return s[:1].upper() + s[1:]

FALLBACK_PRICE_CENTS = 5000  # display only; real charge logic can differ

def price_cents_for_slug(slug: str, plan: str = "enrollment") -> int:
    now = datetime.now(timezone.utc)
    row = db.session.execute(db.text("""
        SELECT p.amount_cents
        FROM auth_pricing p
        JOIN auth_subject s ON s.id = p.subject_id
        WHERE s.slug = :slug
          AND p.plan = :plan
          AND p.is_active = 1
          AND p.active_from <= :now
          AND (p.active_to IS NULL OR p.active_to > :now)
        ORDER BY p.active_from DESC
        LIMIT 1
    """), {"slug": slug, "plan": plan, "now": now}).first()
    return int(row[0]) if row and row[0] is not None else FALLBACK_PRICE_CENTS
