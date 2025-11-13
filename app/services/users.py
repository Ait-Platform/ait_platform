# app/services/users.py
from sqlalchemy import text as sa_text
from app import db

def _ensure_or_create_user_from_session(ctx: dict) -> int:
    """
    Uses staged fields in session["reg_ctx"] to find-or-create the user.
    Expects:
      - ctx["email_lower"]
      - ctx["password_hash"]   (already hashed)
      - ctx["full_name"]       (optional)
    Returns: user_id (int)
    """
    email_lower = (ctx.get("email_lower") or "").strip().lower()
    pw_hash     = (ctx.get("password_hash") or "").strip()
    full_name   = (ctx.get("full_name") or "").strip()

    if not email_lower or not pw_hash:
        raise ValueError("Missing staged email/password in session")

    # 1) Look up existing user by lower(email)
    existing_id = db.session.execute(
        sa_text('SELECT id FROM "user" WHERE lower(email)=:e LIMIT 1'),
        {"e": email_lower}
    ).scalar()
    if existing_id:
        return int(existing_id)

    # 2) Create new user
    db.session.execute(sa_text("""
        INSERT INTO "user" (email, full_name, password_hash, is_active, created_at)
        VALUES (:email, :full_name, :pw_hash, 1, CURRENT_TIMESTAMP)
    """), {
        "email": email_lower,
        "full_name": full_name,
        "pw_hash": pw_hash,
    })
    user_id = db.session.execute(sa_text("SELECT last_insert_rowid()")).scalar()
    db.session.commit()
    return int(user_id)
