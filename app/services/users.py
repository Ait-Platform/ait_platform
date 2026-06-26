# app/services/users.py
from sqlalchemy import text as sa_text
from app.extensions import db



def _ensure_or_create_user_from_session(ctx: dict) -> int:
    """
    Make sure there's a user row for the staged registration context.
    Returns the user_id.
    """
    email = (ctx.get("email") or "").strip().lower()
    if not email:
        raise ValueError("Missing email in registration context")

    # derive a simple display name if not provided
    name = (ctx.get("name") or
            email.split("@", 1)[0].replace(".", " ").replace("_", " ").title()).strip()

    pw_hash = ctx.get("password_hash") or ctx.get("pw_hash") or ""
    if not pw_hash:
        raise ValueError("Missing password hash in registration context")

    # already exists?
    row = db.session.execute(
        sa_text('SELECT id FROM "user" WHERE email = :e'),
        {"e": email},
    ).first()
    if row:
        return int(row.id)

    # ✅ INSERT into `name`, not `full_name`
    db.session.execute(
        sa_text("""
            INSERT INTO "user" (email, name, password_hash, is_active)
            VALUES (:email, :name, :pw_hash, 1)
        """),
        {"email": email, "name": name, "pw_hash": pw_hash},
    )

    db.session.flush()

    new_id = int(db.session.execute(
        sa_text('SELECT id FROM "user" WHERE email = :e'),
        {"e": email},
    ).scalar())

    # --- AUTO ENROLL (NOW GRANTS TRIALS) ---
    # Automatically enroll the new user in any subject flagged as 'auto_enroll'
    auto_enroll_subjects = db.session.execute(
        sa_text("SELECT id, trial_days FROM auth_subject WHERE enroll_policy='auto_enroll' AND is_active=1")
    ).fetchall()

    from datetime import datetime, timedelta
    now_utc = datetime.utcnow()

    for row in auto_enroll_subjects:
        sid = int(row.id)
        # Use subject's configured trial_days, fallback to 15 if missing or 0
        t_days = int(row.trial_days) if getattr(row, 'trial_days', None) else 15
        if t_days <= 0:
            t_days = 15
            
        trial_end_date = now_utc + timedelta(days=t_days)

        db.session.execute(
            sa_text("""
                INSERT INTO user_enrollment (user_id, subject_id, status, trial_end, started_at)
                SELECT :uid, :sid, 'trial', :tend, CURRENT_TIMESTAMP
                WHERE NOT EXISTS (
                    SELECT 1 FROM user_enrollment WHERE user_id = :uid AND subject_id = :sid
                )
            """),
            {"uid": new_id, "sid": sid, "tend": trial_end_date}
        )

    db.session.flush()

    return new_id
