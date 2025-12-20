from datetime import datetime, timedelta
from sqlalchemy import text
from app.extensions import db

def run_budgetcash_daily_jobs() -> None:
    now = datetime.utcnow()

    # purge: expired + 60d inactive
    users = db.session.execute(text("""
        SELECT user_id
          FROM user_entitlement
         WHERE product_slug = 'budgetcash'
           AND :now > COALESCE(paid_until, 'epoch')
           AND :now > COALESCE(trial_end,  'epoch')
           AND COALESCE(last_active, 'epoch') < :cutoff
    """), {
        "now": now,
        "cutoff": now - timedelta(days=60),
    }).scalars().all()

    for uid in users:
        db.session.execute(text("DELETE FROM bud_ledger WHERE user_id = :uid"), {"uid": uid})
        db.session.execute(text("DELETE FROM bud_account WHERE user_id = :uid"), {"uid": uid})
        db.session.execute(text("DELETE FROM bud_group_type WHERE user_id = :uid"), {"uid": uid})
        db.session.execute(text("""
            DELETE FROM user_entitlement
             WHERE user_id = :uid
               AND product_slug = 'budgetcash'
        """), {"uid": uid})

    db.session.commit()
