# app/payments/quote.py
from datetime import datetime
from app.extensions import db
from app.models.auth import UserEnrollment
from app.payments.pricing import price_cents_for


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
