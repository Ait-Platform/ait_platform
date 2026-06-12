from datetime import datetime, timedelta
from decimal import Decimal
from app.models.auth import AuthPaymentLog, AuthSubject, UserEnrollment
from app.extensions import db


from datetime import datetime, timedelta
from decimal import Decimal
from app import db
from app.models import UserEnrollment, AuthPaymentLog, AuthSubject

def repair_payment_logs():
    """
    Repair mismatched or stale payment logs for active enrollments.
    - Normalizes enrollment.status from 'pending_payment' to 'active'
    - Ensures payment logs match quoted/charged amounts
    - Backfills missing or incorrect logs
    """

    # Fetch all enrollments that should be considered active
    enrollments = UserEnrollment.query.filter_by(status="active").all()

    for enr in enrollments:
        # Normalize lingering 'pending_payment' enrollments
        if enr.status == "pending_payment":
            enr.status = "active"
            db.session.add(enr)

        # Expected values: quote wins, fallback to charged
        amount_cents = enr.quoted_amount_cents or enr.charged_amount_cents
        currency = enr.quoted_currency or enr.charged_currency
        expected_amount = (Decimal(amount_cents) / 100) if amount_cents else None

        # Find latest successful payment log
        payment = AuthPaymentLog.query.filter_by(
            enrollment_id=enr.id,
            status="success"
        ).order_by(AuthPaymentLog.valid_until.desc()).first()

        # Check mismatch
        mismatch = (
            not payment or
            payment.currency != currency or
            (payment.amount and payment.amount != expected_amount)
        )

        if mismatch:
            # Delete stale log if present
            if payment:
                db.session.delete(payment)

            # Backfill with correct values
            subject = AuthSubject.query.get(enr.subject_id)
            new_payment = AuthPaymentLog(
                user_id=enr.user_id,
                program=subject.slug,
                enrollment_id=enr.id,
                amount=expected_amount,
                currency=currency,
                transaction_id=f"enr_{enr.id}_repair_{int(datetime.utcnow().timestamp())}",
                status="success",
                valid_from=enr.price_locked_at or datetime.utcnow(),
                valid_until=enr.expires_at or (datetime.utcnow() + timedelta(days=30)),
            )
            db.session.add(new_payment)

    db.session.commit()
    print("Payment logs repaired successfully.")
