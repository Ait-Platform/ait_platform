# app/auth/pricing_helpers.py

from flask import current_app
from sqlalchemy import text

from app.extensions import db
from app.models.auth import AuthSubject


def mark_loss_enrollment_free(enrollment_id: int) -> None:
    """
    Mark a LOSS enrollment as free (ad campaign).
    Keeps pricing columns consistent but zeroed.
    """
    db.session.execute(
        text(
            """
            UPDATE user_enrollment
               SET country_code        = COALESCE(country_code, 'ZA'),
                   quoted_currency     = COALESCE(quoted_currency, 'ZAR'),
                   quoted_amount_cents = COALESCE(quoted_amount_cents, 0),
                   price_version       = COALESCE(price_version, '2025-11'),
                   price_locked_at     = COALESCE(price_locked_at, CURRENT_TIMESTAMP),
                   status              = COALESCE(status, 'active')
             WHERE id = :eid
            """
        ),
        {"eid": enrollment_id},
    )
    db.session.commit()


def get_sms_base_price_cents(default_cents: int = 150000) -> int:
    """
    Return the current base price (in cents) for the SMS subject.
    Falls back to `default_cents` (R1500) if nothing configured.
    """
    subj = AuthSubject.query.filter_by(slug="sms").first()
    if not subj:
        current_app.logger.warning(
            "get_sms_base_price_cents: SMS subject not found; using default."
        )
        return default_cents

    row = db.session.execute(
        text(
            """
            SELECT amount_cents
              FROM auth_pricing
             WHERE subject_id = :sid
               AND role = 'user'
               AND plan = 'enrollment'
               AND is_active = 1          -- works in SQLite + Postgres
             ORDER BY active_from DESC, id DESC
             LIMIT 1
            """
        ),
        {"sid": subj.id},
    ).scalar()

    if row is None:
        current_app.logger.info(
            "get_sms_base_price_cents: no active auth_pricing row; using default."
        )
        return default_cents

    try:
        return int(row)
    except Exception:
        current_app.logger.exception(
            "get_sms_base_price_cents: bad amount_cents=%r; using default.", row
        )
        return default_cents
