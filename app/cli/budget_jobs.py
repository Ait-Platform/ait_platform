# app/cli/budget_jobs.py
from datetime import datetime, timedelta
from flask import current_app
from sqlalchemy import text
from app.extensions import db

def run_budgetcash_daily_jobs():
    now = datetime.utcnow()

    # 1) Reminder: 15 days left in trial
    db.session.execute(text("""
        UPDATE user_entitlement
           SET updated_at = :now
         WHERE product_slug = 'budgetcash'
           AND trial_end BETWEEN :d0 AND :d1
    """), {
        "now": now,
        "d0": now + timedelta(days=14, hours=23),
        "d1": now + timedelta(days=15, hours=1),
    })

    # 2) Final warning: 60 days inactive after expiry → purge
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
        # delete BudgetCash data
        db.session.execute(text("DELETE FROM bud_ledger WHERE user_id = :uid"), {"uid": uid})
        db.session.execute(text("DELETE FROM bud_account WHERE user_id = :uid"), {"uid": uid})
        db.session.execute(text("DELETE FROM bud_group_type WHERE user_id = :uid"), {"uid": uid})

        # remove entitlement
        db.session.execute(text("""
            DELETE FROM user_entitlement
             WHERE user_id = :uid
               AND product_slug = 'budgetcash'
        """), {"uid": uid})

    db.session.commit()
