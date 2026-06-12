# app/payments/quote_store.py

from decimal import Decimal

from flask import session

from app.extensions  import db

def _ctx():
    return session.setdefault("reg_ctx", {})

def clear_quote_for_subject(subject_slug: str) -> None:
    ctx = _ctx()
    q = (ctx.get("quote") or {})
    if (q.get("subject") or "") == subject_slug:
        ctx.pop("quote", None)
        session.modified = True

def get_quote(subject_slug: str) -> dict:
    ctx = _ctx()
    q = (ctx.get("quote") or {})
    if (q.get("subject") or "") != subject_slug:
        return {}
    return q

def set_quote(*, subject_slug: str, country_code: str, local_cents: int, zar_cents: int) -> dict:
    ctx = _ctx()
    q = {
        "subject": subject_slug,
        "country_code": (country_code or "").strip().upper(),
        "local_amount_cents": int(local_cents or 0),
        "final_zar_cents": int(zar_cents or 0),
    }
    ctx["quote"] = q
    session.modified = True
    return q

def fetch_and_store_quote(*, subject_id: int, subject_slug: str, country_code: str) -> tuple[dict, dict]:
    """
    Returns (price_ctx, quote_dict). If missing, returns (blank_ctx, {}).
    NOTE: no is_active checks here (your column type is unstable right now).
    """
    cc = (country_code or "").strip().upper()
    price_ctx = {
        "has_quote": False,
        "country_code": None,
        "local_amount": None,
        "local_currency": None,
        "estimated_zar": None,
        "fx_rate": None,
        "is_discount": False,
    }
    if not cc:
        return price_ctx, {}

    row = db.session.execute(
        db.text("""
            SELECT local_amount_cents, zar_amount_cents
              FROM subject_country_price
             WHERE subject_id   = :sid
               AND country_code = :cc
             LIMIT 1
        """),
        {"sid": int(subject_id), "cc": cc},
    ).mappings().first()

    if not row:
        return price_ctx, {}

    local_cents = int(row["local_amount_cents"] or 0)
    zar_cents   = int(row["zar_amount_cents"] or 0)
    fx = (Decimal(zar_cents) / Decimal(local_cents)) if local_cents else None

    price_ctx = {
        "has_quote": True,
        "country_code": cc,
        "local_amount": (Decimal(local_cents) / Decimal("100")) if local_cents else None,
        "local_currency": "",
        "estimated_zar": (Decimal(zar_cents) / Decimal("100")) if zar_cents else None,
        "fx_rate": (float(fx) if fx is not None else None),
        "is_discount": False,
    }

    q = set_quote(
        subject_slug=subject_slug,
        country_code=cc,
        local_cents=local_cents,
        zar_cents=zar_cents,
    )
    return price_ctx, q
