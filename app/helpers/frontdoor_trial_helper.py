# app/helpers/frontdoor_trial_helper.py

from datetime import timedelta
from sqlalchemy import text as sa_text


def frontdoor_gate_after_register(
    *,
    uid: int,
    sid: int,
    slug: str,
    trial_days: int,
    requires_price: int,
    price_row: dict | None,
    price_version: str,
    db,
):
    """
    FRONT DOOR GATE (LOCKED)

    Rules:
      1) If enrollment.status == 'pending' => PAY FIRST (hard stop)
      2) If trial_days > 0:
           - if trial running => enforce ACTIVE
           - if trial ever existed/used => flip/keep PENDING => PAY FIRST
           - else start trial => enrollment ACTIVE + lock quote
      3) Else if requires_price == 1:
           - enforce PENDING + lock quote => PAY FIRST
      4) Else free => enforce ACTIVE
    """

    uid = int(uid)
    sid = int(sid)
    slug = (slug or "").strip().lower()
    trial_days = int(trial_days or 0)
    requires_price = int(requires_price or 0)
    price_version = (price_version or "2026-01").strip() or "2026-01"

    # ---- read current enrollment snapshot ----
    enr = db.session.execute(sa_text("""
        SELECT status, trial_end, expires_at, trial_count
          FROM user_enrollment
         WHERE user_id = :uid AND subject_id = :sid
         ORDER BY id DESC
         LIMIT 1
    """), {"uid": uid, "sid": sid}).mappings().first()

    enr_status = ((enr.get("status") if enr else "") or "").strip().lower()
    trial_end  = enr.get("trial_end") if enr else None
    expires_at = enr.get("expires_at") if enr else None
    trial_used = bool(enr.get("trial_count")) if enr else False

    now = db.session.execute(sa_text("SELECT CURRENT_TIMESTAMP")).scalar()

    paid_active  = bool(expires_at and expires_at > now)
    trial_active = bool(trial_end and trial_end > now)

    # 1) PAY FIRST hard stop
    if enr_status == "pending":
        return "pending_pay"

    # paid beats everything
    if paid_active:
        _ensure_enrollment_active(uid=uid, sid=sid, db=db, expires_at=None)
        return "enter"

    # 2) TRIAL POLICY
    if trial_days > 0:
        if trial_active:
            _ensure_enrollment_active(uid=uid, sid=sid, db=db, expires_at=trial_end)
            return "enter"

        if trial_used or (trial_end is not None):
            if requires_price == 1:
                _ensure_enrollment_pending_with_quote(
                    uid=uid,
                    sid=sid,
                    db=db,
                    price_row=price_row,
                    price_version=price_version,
                )
                return "pending_pay"
            else:
                _ensure_enrollment_active(uid=uid, sid=sid, db=db, expires_at=None)
                return "enter"

        # start brand new trial
        trial_end_new = now + timedelta(days=trial_days)
        _ensure_enrollment_active_with_quote(
            uid=uid,
            sid=sid,
            db=db,
            expires_at=trial_end_new,
            price_row=price_row,
            price_version=price_version,
        )
        return "enter"

    # 3) PAID POLICY (no trial)
    if requires_price == 1:
        _ensure_enrollment_pending_with_quote(
            uid=uid,
            sid=sid,
            db=db,
            price_row=price_row,
            price_version=price_version,
        )
        return "pending_pay"

    # 4) FREE
    _ensure_enrollment_active(uid=uid, sid=sid, db=db, expires_at=None)
    return "enter"

def _ensure_trial_enrollment(*, uid: int, sid: int, trial_days: int, db):
    """
    Ensure a trial enrollment exists or update existing one.
    - uid: user id
    - sid: subject id
    - trial_days: number of trial days to grant
    """

    enr_id = db.session.execute(sa_text("""
        SELECT id
          FROM user_enrollment
         WHERE user_id = :uid AND subject_id = :sid
         ORDER BY id DESC
         LIMIT 1
    """), {"uid": int(uid), "sid": int(sid)}).scalar()

    if enr_id:
        db.session.execute(sa_text("""
            UPDATE user_enrollment
               SET started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
                   trial_end  = COALESCE(trial_end, CURRENT_TIMESTAMP + (:days || ' days')::interval),
                   trial_count = trial_count + 1,
                   status = 'active',
                   expires_at = trial_end,
                   updated_at = CURRENT_TIMESTAMP
             WHERE id = :id
        """), {"id": int(enr_id), "days": int(trial_days)})
    else:
        db.session.execute(sa_text("""
            INSERT INTO user_enrollment (
                user_id, subject_id,
                status, trial_count,
                started_at, trial_end,
                expires_at, created_at, updated_at
            )
            VALUES (
                :uid, :sid,
                'active', 1,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + (:days || ' days')::interval,
                CURRENT_TIMESTAMP + (:days || ' days')::interval,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
        """), {"uid": int(uid), "sid": int(sid), "days": int(trial_days)})

    db.session.commit()

def _ensure_enrollment_active(*, uid: int, sid: int, db, expires_at):
    """
    Ensure the enrollment row exists and is marked active.
    - uid: user id
    - sid: subject id
    - expires_at: optional datetime for expiry
    """
    db.session.execute(sa_text("""
        INSERT INTO user_enrollment (
            user_id, subject_id, status,
            trial_count, started_at, expires_at, updated_at
        )
        VALUES (:uid, :sid, 'active', 0, CURRENT_TIMESTAMP, :exp, CURRENT_TIMESTAMP)
        ON CONFLICT (user_id, subject_id) DO UPDATE SET
            status     = 'active',
            started_at = COALESCE(user_enrollment.started_at, EXCLUDED.started_at),
            expires_at = COALESCE(user_enrollment.expires_at, EXCLUDED.expires_at),
            updated_at = CURRENT_TIMESTAMP
    """), {"uid": int(uid), "sid": int(sid), "exp": expires_at})
    db.session.commit()

def _ensure_enrollment_pending_with_quote(
    *, uid: int, sid: int, db, price_row: dict | None, price_version: str
):
    """
    Ensure enrollment is marked pending, with a locked quote if available.
    """

    if not price_row:
        # No price row: just enforce pending status
        db.session.execute(sa_text("""
            INSERT INTO user_enrollment (user_id, subject_id, status, updated_at)
            VALUES (:uid, :sid, 'pending', CURRENT_TIMESTAMP)
            ON CONFLICT (user_id, subject_id) DO UPDATE SET
                status     = 'pending',
                updated_at = CURRENT_TIMESTAMP
        """), {"uid": int(uid), "sid": int(sid)})
        db.session.commit()
        return

    db.session.execute(sa_text("""
        INSERT INTO user_enrollment (
            user_id, subject_id, status,
            country_code, quoted_currency, quoted_amount_cents,
            price_version, price_locked_at, updated_at
        )
        VALUES (
            :uid, :sid, 'pending',
            :cc, :qc, :qa,
            :pv, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        )
        ON CONFLICT (user_id, subject_id) DO UPDATE SET
            status              = 'pending',
            country_code        = COALESCE(user_enrollment.country_code,        EXCLUDED.country_code),
            quoted_currency     = COALESCE(user_enrollment.quoted_currency,     EXCLUDED.quoted_currency),
            quoted_amount_cents = COALESCE(user_enrollment.quoted_amount_cents, EXCLUDED.quoted_amount_cents),
            price_version       = COALESCE(user_enrollment.price_version,       EXCLUDED.price_version),
            price_locked_at     = COALESCE(user_enrollment.price_locked_at,     EXCLUDED.price_locked_at),
            updated_at          = CURRENT_TIMESTAMP
    """), {
        "uid": int(uid),
        "sid": int(sid),
        "cc": (price_row.get("country_code") or None),
        "qc": (price_row.get("local_currency") or None),
        "qa": int(price_row.get("local_amount_cents") or 0) or None,
        "pv": (price_version or "2026-01").strip() or "2026-01",
    })
    db.session.commit()

def _ensure_enrollment_active_with_quote(
    *, uid: int, sid: int, db, expires_at, price_row: dict | None, price_version: str
):
    """
    Ensure enrollment is marked active, with a locked quote if available.
    Used for trial-active flows where we want to skip quote later.
    """

    if not price_row:
        # No quote: still make them active for trial
        _ensure_enrollment_active(uid=uid, sid=sid, db=db, expires_at=expires_at)
        return

    db.session.execute(sa_text("""
        INSERT INTO user_enrollment (
            user_id, subject_id, status,
            started_at, expires_at,
            country_code, quoted_currency, quoted_amount_cents,
            price_version, price_locked_at, updated_at
        )
        VALUES (
            :uid, :sid, 'active',
            CURRENT_TIMESTAMP, :exp,
            :cc, :qc, :qa,
            :pv, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        )
                               
                               
        ON CONFLICT (user_id, subject_id) DO UPDATE SET
            status     = 'active',
            started_at = COALESCE(user_enrollment.started_at, EXCLUDED.started_at),
            expires_at = COALESCE(user_enrollment.expires_at, EXCLUDED.expires_at),
            country_code        = COALESCE(user_enrollment.country_code,        EXCLUDED.country_code),
            quoted_currency     = COALESCE(user_enrollment.quoted_currency,     EXCLUDED.quoted_currency),
            quoted_amount_cents = COALESCE(user_enrollment.quoted_amount_cents, EXCLUDED.quoted_amount_cents),
            price_version       = COALESCE(user_enrollment.price_version,       EXCLUDED.price_version),
            price_locked_at     = COALESCE(user_enrollment.price_locked_at,     EXCLUDED.price_locked_at),
            updated_at          = CURRENT_TIMESTAMP
    """), {
        "uid": int(uid),
        "sid": int(sid),
        "exp": expires_at,
        "cc": (price_row.get("country_code") or None),
        "qc": (price_row.get("local_currency") or None),
        "qa": int(price_row.get("local_amount_cents") or 0) or None,
        "pv": (price_version or "2026-01").strip() or "2026-01",
    })
    db.session.commit()

