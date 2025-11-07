# services/loss_result.py (or inside your loss blueprint module)
from sqlalchemy import text
from datetime import datetime
from flask import current_app as app
from app import db  # adjust import to your project

def compute_and_upsert_loss_result(run_id: int) -> dict | None:
    """
    Computes phase totals for a run from lca_scorecard_v (or fall back to lca_response join),
    then inserts/updates lca_result. Returns the row dict or None if nothing to compute.
    """

    # 1) Pull per-phase totals from your scoring view/table.
    # Prefer your *existing* scoring view if you have it (you mentioned lca_scorecard_v).
    # Expected columns in the query result: p1, p2, p3, p4, user_id
    row = db.session.execute(
        text("""
            SELECT
              COALESCE(SUM(CASE WHEN phase = 1 THEN score ELSE 0 END), 0) AS p1,
              COALESCE(SUM(CASE WHEN phase = 2 THEN score ELSE 0 END), 0) AS p2,
              COALESCE(SUM(CASE WHEN phase = 3 THEN score ELSE 0 END), 0) AS p3,
              COALESCE(SUM(CASE WHEN phase = 4 THEN score ELSE 0 END), 0) AS p4,
              MAX(user_id) AS user_id
            FROM lca_scorecard_v
            WHERE run_id = :rid
        """),
        {"rid": run_id}
    ).mappings().first()

    if not row:
        return None

    p1, p2, p3, p4 = int(row["p1"] or 0), int(row["p2"] or 0), int(row["p3"] or 0), int(row["p4"] or 0)
    total = p1 + p2 + p3 + p4
    user_id = row["user_id"] or 0

    # 2) Upsert into lca_result.
    # If you have a UNIQUE constraint on lca_result.run_id you can use ON CONFLICT in SQLite.
    # If not, do a manual UPDATE/INSERT path.
    try:
        db.session.execute(
            text("""
                INSERT INTO lca_result (user_id, run_id, phase_1, phase_2, phase_3, phase_4, total, subject, created_at)
                VALUES (:user_id, :rid, :p1, :p2, :p3, :p4, :total, 'LOSS', :now)
                ON CONFLICT(run_id) DO UPDATE SET
                    user_id = excluded.user_id,
                    phase_1 = excluded.phase_1,
                    phase_2 = excluded.phase_2,
                    phase_3 = excluded.phase_3,
                    phase_4 = excluded.phase_4,
                    total   = excluded.total
            """),
            {"user_id": user_id, "rid": run_id, "p1": p1, "p2": p2, "p3": p3, "p4": p4, "total": total, "now": datetime.utcnow().isoformat(sep=" ", timespec="seconds")}
        )
    except Exception:
        # Fallback if run_id isn’t unique. Update if exists, else insert.
        existing = db.session.execute(
            text("SELECT id FROM lca_result WHERE run_id = :rid"),
            {"rid": run_id}
        ).first()
        if existing:
            db.session.execute(
                text("""
                    UPDATE lca_result
                       SET user_id=:user_id, phase_1=:p1, phase_2=:p2, phase_3=:p3, phase_4=:p4, total=:total, subject='LOSS'
                     WHERE run_id=:rid
                """),
                {"user_id": user_id, "rid": run_id, "p1": p1, "p2": p2, "p3": p3, "p4": p4, "total": total}
            )
        else:
            db.session.execute(
                text("""
                    INSERT INTO lca_result (user_id, run_id, phase_1, phase_2, phase_3, phase_4, total, subject, created_at)
                    VALUES (:user_id, :rid, :p1, :p2, :p3, :p4, :total, 'LOSS', :now)
                """),
                {"user_id": user_id, "rid": run_id, "p1": p1, "p2": p2, "p3": p3, "p4": p4, "total": total, "now": datetime.utcnow().isoformat(sep=" ", timespec="seconds")}
            )

    db.session.commit()

    return {
        "user_id": user_id, "run_id": run_id,
        "phase_1": p1, "phase_2": p2, "phase_3": p3, "phase_4": p4, "total": total
    }
