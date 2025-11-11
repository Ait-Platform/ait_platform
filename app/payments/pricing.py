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

FALLBACK_PRICE_CENTS = 5000  # only used if DB has no price rows at all

def _client_iso2() -> str:
    # Allow ?cc=ZA for quick testing; else Cloudflare header; else default ZA
    return (request.args.get("cc") or request.headers.get("cf-ipcountry") or "ZA").upper()

def _currency_for_iso2(iso2: str) -> str | None:
    row = db.session.execute(
        db.text("SELECT currency FROM country_currency WHERE iso2 = :iso2"),
        {"iso2": iso2}
    ).first()
    return row[0] if row else None

def price_cents_for(subject_slug: str, iso2: str | None = None, role: str | None = "user",
                    plan: str = "enrollment") -> int:
    subj = AuthSubject.query.filter_by(slug=subject_slug).first()
    if not subj:
        return FALLBACK_PRICE_CENTS

    iso2 = (iso2 or _client_iso2()).upper()
    wanted_currency = _currency_for_iso2(iso2)

    now = datetime.now(timezone.utc)

    def _query(currency: str | None):
        q = (AuthPricing.query
             .filter(AuthPricing.subject_id == subj.id,
                     AuthPricing.plan == plan,
                     AuthPricing.is_active == 1,
                     AuthPricing.active_from <= now,
                     (AuthPricing.active_to.is_(None)) | (AuthPricing.active_to > now))
             .order_by(AuthPricing.active_from.desc()))
        if currency:
            q = q.filter(AuthPricing.currency == currency)
        rec = q.filter((AuthPricing.role == role) | (AuthPricing.role.is_(None))).first()
        return int(rec.amount_cents) if rec and rec.amount_cents else None

    # 1) Try desired currency from DB mapping
    if wanted_currency:
        amt = _query(wanted_currency)
        if amt:
            return amt

    # 2) Try any active price row for the subject/plan (e.g., ZAR)
    any_amt = _query(None)
    if any_amt:
        return any_amt

    # 3) Final fallback
    return FALLBACK_PRICE_CENTS
