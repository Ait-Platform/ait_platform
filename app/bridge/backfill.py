
from datetime import datetime, timedelta
from decimal import Decimal

from app.models.auth import AuthPaymentLog
from app.extensions import db


def backfill_payment_log(enrollment, subject):
    # Always prefer the simplified local + ZAR fields
    amount_cents = enrollment.zar_amount_cents
    currency = "ZAR"  # Yoco always charges in ZAR

    new_payment = AuthPaymentLog(
        user_id=enrollment.user_id,
        program=subject.slug,
        enrollment_id=enrollment.id,
        amount=(Decimal(amount_cents) / 100 if amount_cents else None),
        currency=currency,
        transaction_id=f"enr_{enrollment.id}_backfill",
        status="success",
        valid_from=enrollment.price_locked_at or datetime.utcnow(),
        valid_until=enrollment.expires_at or (datetime.utcnow() + timedelta(days=30)),
        local_currency=enrollment.local_currency,
        local_amount_cents=enrollment.local_amount_cents,
        price_id=enrollment.price_id,
        country_code=enrollment.country_code,
    )
    db.session.add(new_payment)
    db.session.commit()
    return new_payment
