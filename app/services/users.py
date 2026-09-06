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

    # Create AitTokenWallet with 100 tokens for new users (Registration Trial Bonus)
    # Exclude SACE Endorsement users from receiving tokens/wallets
    subject = (ctx.get("subject") or "").strip().lower()
    if not subject.startswith("sace"):
        db.session.execute(
            sa_text("""
                INSERT INTO ait_token_wallet (user_id, balance, created_at)
                VALUES (:user_id, 100, CURRENT_TIMESTAMP)
            """),
            {"user_id": new_id}
        )
        db.session.flush()
        
        wallet_id = int(db.session.execute(
            sa_text('SELECT id FROM ait_token_wallet WHERE user_id = :u'),
            {"u": new_id}
        ).scalar())

        db.session.execute(
            sa_text("""
                INSERT INTO ait_token_transaction (wallet_id, amount, description, created_at)
                VALUES (:wallet_id, 100, 'Registration Trial Bonus', CURRENT_TIMESTAMP)
            """),
            {"wallet_id": wallet_id}
        )

    db.session.commit()

    return new_id
