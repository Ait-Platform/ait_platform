# payments/AuthPricing.py
from __future__ import annotations
from datetime import datetime
from flask import current_app
from sqlalchemy import and_, or_, select
from app.models.auth import AuthPricing, AuthSubject, User
from app.extensions import db
from sqlalchemy import inspect, text

__all__ = ["price_cents_for", "price_dict_for"]

def price_cents_for(subject_slug: str, currency: str = "ZAR") -> int | None:
    """
    Return the active price (in cents) for a subject + currency, or None if none found.
    """
    row = db.session.execute(
        """
        SELECT p.amount_cents
        FROM auth_pricing p
        JOIN auth_subject s ON s.id = p.subject_id
        WHERE s.slug = :slug
          AND p.currency = :cur
          AND p.is_active = 1
          AND (p.active_from IS NULL OR p.active_from <= CURRENT_TIMESTAMP)
          AND (p.active_to   IS NULL OR p.active_to   >  CURRENT_TIMESTAMP)
        ORDER BY p.active_from DESC
        LIMIT 1
        """,
        {"slug": subject_slug, "cur": currency},
    ).fetchone()
    return int(row[0]) if row else None

def price_dict_for(subject_slug: str, currency: str = "ZAR") -> dict | None:
    """
    Convenience wrapper for templates: returns {"currency": "...", "amount_cents": N} or None.
    """
    cents = price_cents_for(subject_slug, currency)
    return {"currency": currency, "amount_cents": cents} if cents is not None else None

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

def enrollment_status_for(*, user_id: int, subject_slug: str) -> str:
    row = db.session.execute(
        db.text("""
            SELECT ue.status
            FROM user_enrollment ue
            JOIN auth_subject s ON s.id = ue.subject_id
            WHERE ue.user_id = :uid
              AND lower(s.slug) = :slug
            LIMIT 1
        """),
        {"uid": int(user_id), "slug": (subject_slug or "").strip().lower()},
    ).first()
    return (row.status if row else "") or ""

def _table_has_column(table_name: str, column_name: str) -> bool:
    try:
        insp = inspect(db.engine)
        cols = {c["name"] for c in insp.get_columns(table_name)}
        return column_name in cols
    except Exception:
        return False

def _resolve_subject_id(subject_slug: str) -> int | None:
    s = (subject_slug or "").strip().lower()
    if not s:
        return None

    try:
        return db.session.execute(
            text(
                """
                SELECT id
                  FROM auth_subject
                 WHERE lower(slug) = :s
                    OR lower(name) = :s
                 LIMIT 1
                """
            ),
            {"s": s},
        ).scalar()
    except Exception:
        current_app.logger.exception("YOCO: failed resolving subject id for %s", s)
        return None

def _ensure_user(*, email: str, ctx: dict) -> User:
    """
    Ensure a User exists for email.
    If reg_ctx staged a password_hash during /register, apply it on first creation.
    """
    email = (email or "").strip().lower()
    u = User.query.filter_by(email=email).first()
    if u:
        return u

    staged_hash = (ctx.get("password_hash") or "").strip() or None
    display = (
        (ctx.get("full_name") or "").strip()
        or email.split("@", 1)[0].replace(".", " ").replace("_", " ").title()
    )

    u = User(email=email, name=display, is_active=1)
    if staged_hash:
        u.password_hash = staged_hash

    db.session.add(u)
    db.session.flush()  # get u.id
    return u

def _ensure_enrollment_active(*, user_id: int, subject_id: int) -> int:
    """
    Ensure user_enrollment exists and is active.
    If schema has paid_at, set it; otherwise just status.
    Returns enrollment id.
    """
    has_paid_at = _table_has_column("user_enrollment", "paid_at")

    row = db.session.execute(
        text(
            """
            SELECT id, status
              FROM user_enrollment
             WHERE user_id = :uid
               AND subject_id = :sid
             LIMIT 1
            """
        ),
        {"uid": int(user_id), "sid": int(subject_id)},
    ).mappings().first()

    if row:
        if has_paid_at:
            db.session.execute(
                text(
                    """
                    UPDATE user_enrollment
                       SET status = 'active',
                           paid_at = CURRENT_TIMESTAMP
                     WHERE id = :eid
                    """
                ),
                {"eid": int(row["id"])},
            )
        else:
            db.session.execute(
                text(
                    """
                    UPDATE user_enrollment
                       SET status = 'active'
                     WHERE id = :eid
                    """
                ),
                {"eid": int(row["id"])},
            )
        return int(row["id"])

    if has_paid_at:
        eid = db.session.execute(
            text(
                """
                INSERT INTO user_enrollment (user_id, subject_id, status, paid_at)
                VALUES (:uid, :sid, 'active', CURRENT_TIMESTAMP)
                RETURNING id
                """
            ),
            {"uid": int(user_id), "sid": int(subject_id)},
        ).scalar_one()
    else:
        eid = db.session.execute(
            text(
                """
                INSERT INTO user_enrollment (user_id, subject_id, status)
                VALUES (:uid, :sid, 'active')
                RETURNING id
                """
            ),
            {"uid": int(user_id), "sid": int(subject_id)},
        ).scalar_one()

    return int(eid)
