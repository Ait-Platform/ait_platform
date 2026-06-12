#app/enrollment/helpers.py
from datetime import datetime
from datetime import datetime
from flask import session
from app.quote.routes import get_baton_context



def persist_subject_baton(slug: str):
    """
    Ensure baton carries the subject slug and is persisted in session.
    Call this in any subject route (about, quote, register).
    """
    baton = get_baton_context()
    baton["subject_slug"] = slug.strip().lower()
    session["baton"] = baton
    return baton

def lock_quote_into_enrollment(enrollment, price_row):
    """
    Ensure enrollment carries full quote details for checkout.
    """
    if not price_row:
        return enrollment

    # Core identifiers
    enrollment.price_id = price_row.id                # ✅ persist price_id
    enrollment.country_code = price_row.country_code

    # Quoted values (locked at time of enrollment)
    enrollment.quoted_currency = price_row.local_currency
    if price_row.local_amount is not None:
        enrollment.quoted_amount_cents = int(price_row.local_amount * 100)

    # Optional local values if you want to persist them
    if hasattr(enrollment, "local_currency"):
        enrollment.local_currency = price_row.local_currency
    if hasattr(enrollment, "local_amount_cents") and price_row.local_amount is not None:
        enrollment.local_amount_cents = int(price_row.local_amount * 100)

    # Price version + lock timestamp
    enrollment.price_version = getattr(price_row, "version", None)
    enrollment.price_locked_at = datetime.utcnow()

    return enrollment

def normalize_enrollment(row) -> dict:
    """
    Normalize an enrollment row into a dict for downstream use.
    Works whether row comes from Charlie confirm or a direct query.
    """
    return {
        "id": int(row["id"]),
        "user_id": int(row["user_id"]),
        "subject_slug": row.get("subject_slug"),
        "price_id": int(row.get("price_id") or 0),
        "country_code": row.get("country_code"),
        "quoted_amount_cents": int(row.get("quoted_amount_cents") or 0),
        "quoted_currency": row.get("quoted_currency"),
        "status": (row.get("status") or "").strip().lower(),
        "trial_count": int(row.get("trial_count") or 0),
        "trial_end": row.get("trial_end"),
    }

