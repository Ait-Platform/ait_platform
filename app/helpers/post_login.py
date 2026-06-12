# app/helpers/---
# app/helpers/bridge_state.py
from datetime import datetime, timezone
from app.extensions import db
from app.models.auth import AuthSubject, UserEnrollment

def post_login_next_url(*, user_id: int, email: str) -> str:
    """
    Post-login router:
      - subject_id is authoritative from user_enrollment
      - is consulted only as an additional signal (never required to exist)
      - BudgetCash first
      - Always go to BRIDGE (guard)
    """
    from flask import url_for
    from sqlalchemy import text as sa_text
    from app import db

    uid = int(user_id)
    eml = (email or "").strip().lower() or ""

    def _go_bridge(subject_id: int, subject_key: str) -> str:
        return url_for(
            "general_bp.bridge",
            subject_id=int(subject_id),
            subject=(subject_key or "").strip().lower(),
            email=eml,
        )
    def clear_stale_pending_enrollments(*, user_id: int) -> None:
        """
        If an enrollment is 'pending' but there is no active trial/paid window,
        reset it to 'locked'. Pending is transient; it must not persist across sessions.
        """
        from sqlalchemy import text as sa_text
        from app import db

        uid = int(user_id)

        # If no expires_at and no trial_end, pending is stale
        db.session.execute(sa_text("""
            UPDATE user_enrollment ue
            SET status = 'locked'
            WHERE ue.user_id = :uid
            AND lower(coalesce(ue.status,'')) = 'pending'
            AND (
                    (ue.expires_at IS NULL OR ue.expires_at <= NOW())
                AND (ue.trial_end IS NULL OR ue.trial_end <= NOW())
            )
        """), {"uid": uid})

        db.session.commit()

def get_bridge_state_for_user(email: str):
    email = (email or "").strip().lower()
    now = datetime.utcnow()

    rows = (
        db.session.query(AuthSubject, UserEnrollment)
        .join(UserEnrollment, UserEnrollment.subject_id == AuthSubject.id)
        .filter(UserEnrollment.user.has(email=email))
        .filter(AuthSubject.is_active == 1)
        .order_by(
            db.case((db.func.lower(AuthSubject.slug).in_(["budget", "budgetcash"]), 0), else_=1),
            AuthSubject.sort_order,
            AuthSubject.name,
        )
        .all()
    )

    out = []

    for s, e in rows:
        decision = "LOCKED"

        if e.status == "pending":
            # Pending → either quote or pay depending on fields
            if e.quoted_amount_cents and e.quoted_currency:
                decision = "PAY"
            else:
                decision = "QUOTE"

        elif s.commercial_mode == "free" and e.status == "active":
            decision = "BRIDGE"

        elif s.commercial_mode == "trial":
            if e.trial_end and e.trial_end > now and e.status == "active":
                decision = "BRIDGE"
            else:
                decision = "QUOTE"

        elif s.commercial_mode == "paid":
            if e.expires_at and e.expires_at > now and e.status == "active":
                decision = "BRIDGE"
            else:
                decision = "PAY"

        out.append(
            {
                "subject_id": s.id,
                "slug": s.slug,
                "name": s.name,
                "decision": decision,
                "trial_end": e.trial_end,
                "expires_at": e.expires_at,
                "enrollment_status": e.status,
            }
        )

    return out