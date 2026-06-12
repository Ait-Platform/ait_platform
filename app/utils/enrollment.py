# app/utils/enrollment.py
from app.extensions import db
from flask import current_app, session
from app.auth.helpers import subject_id_from_slug
from sqlalchemy import text, bindparam
from app.models.auth import AuthSubject, UserEnrollment
from app.models.payment import SubjectCountryPrice

def _slug_from_id(subject_id: int) -> str | None:
    row = db.session.execute(
        text("SELECT slug FROM auth_subject WHERE id = :sid LIMIT 1"),
        {"sid": subject_id},
    ).fetchone()
    return (row[0] if row else None)

def create_pending_user_enrollment(*, user_id: int, subject_slug: str, program: str | None):
    """Idempotent: ensure a pending enrollment (payment_pending=1) in user_enrollment for the subject."""
    subject_slug = (subject_slug or "").lower().strip()
    sid = subject_id_from_slug(subject_slug)
    if not sid:
        current_app.logger.warning("create_pending_user_enrollment: subject not found '%s'", subject_slug)
        return
    sql = text("""
        INSERT INTO user_enrollment (
            user_id, program, current_chapter, payment_pending, completed,
            subject_id, status, created_at
        )
        VALUES (:uid, :program, NULL, 1, 0, :sid, 'active', datetime('now'))
        ON CONFLICT(user_id, subject_id) DO UPDATE SET
            program         = COALESCE(excluded.program, user_enrollment.program),
            status          = 'active',
            payment_pending = 1;
    """)
    db.session.execute(sql, {"uid": user_id, "sid": sid, "program": (program or subject_slug)})
    db.session.commit()

def settle_user_enrollment_paid(*, user_id: int, subject_slug: str, program: str | None):
    """Idempotent: mark enrollment as paid (payment_pending=0, status=active) in user_enrollment."""
    subject_slug = (subject_slug or "").lower().strip()
    sid = subject_id_from_slug(subject_slug)
    if not sid:
        current_app.logger.warning("settle_user_enrollment_paid: subject not found '%s'", subject_slug)
        return
    sql = text("""
        INSERT INTO user_enrollment (
            user_id, program, current_chapter, payment_pending, completed,
            subject_id, status, created_at
        )
        VALUES (:uid, :program, NULL, 0, 0, :sid, 'active', datetime('now'))
        ON CONFLICT(user_id, subject_id) DO UPDATE SET
            program         = COALESCE(excluded.program, user_enrollment.program),
            status          = 'active',
            payment_pending = 0;
    """)
    db.session.execute(sql, {"uid": user_id, "sid": sid, "program": (program or subject_slug)})
    db.session.commit()

def ensure_pending_enrollment(user_id: int, subject_id: int, program: str | None = None):
    """
    Back-compat wrapper:
    Ensure ACTIVE + pending enrollment using subject_id.
    (Delegates to slug-based canonical.)
    """
    slug = _slug_from_id(subject_id)
    if not slug:
        current_app.logger.warning("ensure_pending_enrollment: subject_id not found '%s'", subject_id)
        return
    create_pending_user_enrollment(user_id=user_id, subject_slug=slug, program=program or slug)

def mark_payment_settled(user_id: int, subject_id: int):
    """
    Back-compat wrapper:
    Mark enrollment as paid using subject_id.
    (Delegates to slug-based canonical.)
    """
    slug = _slug_from_id(subject_id)
    if not slug:
        current_app.logger.warning("mark_payment_settled: subject_id not found '%s'", subject_id)
        return
    settle_user_enrollment_paid(user_id=user_id, subject_slug=slug, program=slug)

def ensure_enrollment(user_id: int, subject_slug: str, role: str):
    """
    Back-compat wrapper:
    Ensure ACTIVE + pending enrollment using subject_slug.
    (Ignores role; settlement happens via Stripe success/webhook.)
    """
    slug = (subject_slug or "").strip().lower()
    create_pending_user_enrollment(user_id=user_id, subject_slug=slug, program=slug)

ENROLLED_STATI = ("active", "paid")

def is_enrolled(user_id: int, subject_id: int) -> bool:
    row = db.session.execute(text("""
        SELECT 1
        FROM user_enrollment
        WHERE user_id   = :uid
          AND subject_id= :sid
          AND status IN ('active','paid')
        LIMIT 1
    """), {"uid": user_id, "sid": subject_id}).fetchone()
    return row is not None

def ensure_free_enrollment(user_id: int, subject_slug: str):
    """
    Ensure a one-time ACTIVE enrollment for a free subject.
    Uses trial_count to block repeat enrollments.
    No redirects or flash messages here — just DB state.
    """
    slug = (subject_slug or "").strip().lower()
    sid = subject_id_from_slug(slug)
    if not sid:
        current_app.logger.warning("ensure_free_enrollment: subject not found '%s'", slug)
        return False  # signal failure

    existing = db.session.execute(
        text("SELECT trial_count FROM user_enrollment WHERE user_id=:uid AND subject_id=:sid"),
        {"uid": user_id, "sid": sid}
    ).fetchone()

    if existing and existing.trial_count >= 1:
        return False  # already consumed free enrollment

    sql = text("""
        INSERT INTO user_enrollment (
            user_id,
            subject_id,
            status,
            started_at,
            trial_count,
            updated_at
        )
        VALUES (
            :uid,
            :sid,
            'active',
            NOW(),
            1,
            NOW()
        )
        ON CONFLICT (user_id, subject_id) DO UPDATE SET
            status      = 'active',
            trial_count = 1,
            updated_at  = NOW();
    """)
    db.session.execute(sql, {"uid": user_id, "sid": sid})
    db.session.commit()

    enrollment = UserEnrollment.query.filter_by(user_id=user_id, subject_id=sid).first()
    session["enrollment_dict"] = {
        "id": enrollment.id,
        "user_id": enrollment.user_id,
        "subject_id": enrollment.subject_id,
        "subject_slug": slug,
        "status": enrollment.status,
        "trial_end": enrollment.trial_end,
        "trial_count": enrollment.trial_count,
        "price_id": enrollment.price_id,
        "country_code": enrollment.country_code,
    }
    return enrollment

def ensure_trial_enrollment(
    user_id: int,
    subject_slug: str,
    max_trials: int = 1,
    trial_days: int = 14,
    country_code: str = None
):
    """
    Ensure an ACTIVE trial enrollment for a subject.
    - On first run: create enrollment with trial_count=1 and trial dates.
    - On subsequent runs: do nothing if already enrolled, block if limit reached.
    """
    slug = (subject_slug or "").strip().lower()
    sid = subject_id_from_slug(slug)
    if not sid:
        current_app.logger.warning("ensure_trial_enrollment: subject not found '%s'", slug)
        return False

    existing = db.session.execute(
        text("SELECT trial_count FROM user_enrollment WHERE user_id=:uid AND subject_id=:sid"),
        {"uid": user_id, "sid": sid}
    ).fetchone()

    if existing and existing.trial_count and existing.trial_count >= max_trials:
        return False

    new_trial_count = 1 if not existing or not existing.trial_count else existing.trial_count + 1

    # Look up price row for this subject/country
    price_row = None
    if country_code:
        price_row = SubjectCountryPrice.query.filter_by(
            subject_id=sid,
            country_code=country_code
        ).first()

    sql = text("""
        INSERT INTO user_enrollment (
            user_id,
            subject_id,
            status,
            started_at,
            expires_at,
            trial_count,
            trial_end,
            email_status,
            updated_at,
            price_id
        )
        VALUES (
            :uid,
            :sid,
            'active',
            NOW(),
            NOW() + (:trial_days * INTERVAL '1 day'),
            1,
            NOW() + (:trial_days * INTERVAL '1 day'),
            'pending',
            NOW(),
            :price_id
        )
        ON CONFLICT (user_id, subject_id) DO UPDATE SET
            status      = 'active',
            trial_count = :trial_count,
            started_at  = NOW(),
            expires_at  = NOW() + (:trial_days * INTERVAL '1 day'),
            trial_end   = NOW() + (:trial_days * INTERVAL '1 day'),
            email_status= 'pending',
            updated_at  = NOW(),
            price_id    = :price_id;
    """)

    db.session.execute(sql, {
        "uid": user_id,
        "sid": sid,
        "trial_count": new_trial_count,
        "trial_days": trial_days,
        "price_id": price_row.id if price_row else None,
    })
    db.session.commit()

    enrollment = UserEnrollment.query.filter_by(user_id=user_id, subject_id=sid).first()
    session["enrollment_dict"] = {
        "id": enrollment.id,
        "user_id": enrollment.user_id,
        "subject_id": enrollment.subject_id,
        "subject_slug": slug,
        "status": enrollment.status,
        "trial_end": enrollment.trial_end,
        "trial_count": enrollment.trial_count,
        "price_id": enrollment.price_id,
        "country_code": enrollment.country_code,
        "quoted_currency": enrollment.quoted_currency,
        "quoted_amount_cents": enrollment.quoted_amount_cents,
    }
    return enrollment

def merge_enrollment_payload(enrollment, price_row):
    """
    Merge pricing context from SubjectCountryPrice into a UserEnrollment.
    Ensures all critical fields are populated for audit and checkout.
    """

    # Core linkage
    enrollment.price_id = price_row.id
    enrollment.country_code = price_row.country_code

    # Quoted values (local currency)
    enrollment.quoted_currency = price_row.local_currency or "USD"
    enrollment.quoted_amount_cents = price_row.local_amount_cents or 0

    # Local values (mirror quoted if schema requires)
    enrollment.local_currency = price_row.local_currency or enrollment.quoted_currency
    enrollment.local_amount_cents = price_row.local_amount_cents or enrollment.quoted_amount_cents

    # Charged values (base currency, e.g. ZAR for Yoco)
    enrollment.charged_currency = "ZAR"
    enrollment.charged_amount_cents = price_row.zar_amount_cents or 0

    # Versioning / audit
    enrollment.price_version = getattr(price_row, "version", price_row.created_at)
    enrollment.price_locked_at = datetime.utcnow()

    # Status defaults
    if not enrollment.status:
        enrollment.status = "pending"

    return enrollment

def ensure_paid_pending_enrollment(
    user_id: int,
    subject_id: int | None = None,
    subject_slug: str | None = None,
    program: str | None = None,
    country_code: str | None = None
):
    """
    Ensure a pending paid enrollment with pricing metadata.
    Works with either subject_id or subject_slug.
    """

    # Resolve subject_id if only slug is given
    if subject_id is None and subject_slug:
        subject_id = subject_id_from_slug(subject_slug.lower().strip())
    elif subject_slug is None and subject_id:
        subject_slug = _slug_from_id(subject_id)

    if not subject_id or not subject_slug:
        current_app.logger.warning(
            "ensure_paid_pending_enrollment: subject not found (id=%s, slug=%s)",
            subject_id, subject_slug
        )
        return None

    # Look up price row for the given subject/country
    price_row = None
    if country_code:
        price_row = db.session.execute(
            text("""
                SELECT id, country_code, local_currency, local_amount_cents,
                       zar_amount_cents, created_at, price_version
                FROM subject_country_price
                WHERE subject_id=:sid AND country_code=:cc
                LIMIT 1
            """),
            {"sid": subject_id, "cc": country_code}
        ).fetchone()

    if not price_row:
        current_app.logger.warning(
            "ensure_paid_pending_enrollment: no price row found (subject_id=%s, country_code=%s)",
            subject_id, country_code
        )
        return None

    # Insert or update enrollment with merged payload
    sql = text("""
        INSERT INTO user_enrollment (
            user_id, subject_id, status, started_at,
            country_code, price_id, quoted_currency, quoted_amount_cents,
            charged_currency, charged_amount_cents,
            price_version, price_locked_at, updated_at
        )
        VALUES (
            :uid, :sid, 'pending', NOW(),
            :cc, :price_id, :quoted_currency, :quoted_amount_cents,
            :charged_currency, :charged_amount_cents,
            :price_version, NOW(), NOW()
        )
        ON CONFLICT(user_id, subject_id) DO UPDATE SET
            status              = 'pending',
            country_code        = :cc,
            price_id            = :price_id,
            quoted_currency     = :quoted_currency,
            quoted_amount_cents = :quoted_amount_cents,
            charged_currency    = :charged_currency,
            charged_amount_cents= :charged_amount_cents,
            price_version       = :price_version,
            price_locked_at     = NOW(),
            updated_at          = NOW();
    """)

    db.session.execute(sql, {
        "uid": user_id,
        "sid": subject_id,
        "cc": country_code,
        "price_id": price_row.id,
        "quoted_currency": price_row.local_currency or "USD",
        "quoted_amount_cents": price_row.local_amount_cents or 0,
        "charged_currency": "ZAR",
        "charged_amount_cents": price_row.zar_amount_cents or 0,
        "price_version": getattr(price_row, "version", price_row.created_at),
    })
    db.session.commit()

    return UserEnrollment.query.filter_by(
        user_id=user_id,
        subject_id=subject_id
    ).first()
