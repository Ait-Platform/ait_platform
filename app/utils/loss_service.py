# app/utils/loss_service.py
from sqlalchemy import text
from flask import session
from app import db

# ---------- Runs ----------
def create_run(user_id: int) -> int:
    db.session.execute(text("""
        INSERT INTO lca_run (user_id, started_at, status)
        VALUES (:uid, datetime('now'), 'in_progress')
    """), {"uid": user_id})
    rid = db.session.execute(text("SELECT last_insert_rowid()")).scalar()
    db.session.commit()
    session["loss_run_id"] = int(rid)
    return int(rid)

def finish_run(run_id: int) -> None:
    db.session.execute(text("""
        UPDATE lca_run SET finished_at = datetime('now'), status='finished'
        WHERE id = :rid
    """), {"rid": int(run_id)})
    db.session.commit()
    if session.get("loss_run_id") == run_id:
        session.pop("loss_run_id", None)

def latest_run_id(user_id: int) -> int | None:
    return db.session.execute(text("""
        SELECT id FROM lca_run
        WHERE user_id = :uid
        ORDER BY id DESC LIMIT 1
    """), {"uid": user_id}).scalar()

def list_runs(user_id: int, limit: int = 20):
    return db.session.execute(text("""
        SELECT id, status, started_at, finished_at
        FROM lca_run
        WHERE user_id = :uid
        ORDER BY id DESC
        LIMIT :lim
    """), {"uid": user_id, "lim": limit}).mappings().all()

# ---------- Responses ----------
def save_answer(user_id: int, run_id: int, question_id: int, answer: str) -> None:
    db.session.execute(text("""
        INSERT INTO lca_response (user_id, run_id, question_id, answer)
        VALUES (:uid, :rid, :qid, :ans)
        ON CONFLICT(run_id, question_id)
        DO UPDATE SET answer = excluded.answer
    """), {"uid": user_id, "rid": run_id, "qid": question_id, "ans": (answer or "").strip().lower()})
    db.session.commit()

def responses_for_run(run_id: int):
    return db.session.execute(text("""
        SELECT r.question_id AS qid, r.answer,
               CASE WHEN lower(r.answer)='yes' THEN sd.p1 ELSE 0 END AS p1,
               CASE WHEN lower(r.answer)='yes' THEN sd.p2 ELSE 0 END AS p2,
               CASE WHEN lower(r.answer)='yes' THEN sd.p3 ELSE 0 END AS p3,
               CASE WHEN lower(r.answer)='yes' THEN sd.p4 ELSE 0 END AS p4
        FROM lca_response r
        JOIN lca_score_definitions sd ON sd.question_id = r.question_id
        WHERE r.run_id = :rid
        ORDER BY r.question_id
    """), {"rid": run_id}).mappings().all()

# ---------- Totals / Scorecard ----------
def totals_for_run(run_id: int):
    return db.session.execute(text("""
        SELECT
          SUM(CASE WHEN lower(r.answer)='yes' THEN sd.p1 ELSE 0 END) AS p1_raw,
          SUM(CASE WHEN lower(r.answer)='yes' THEN sd.p2 ELSE 0 END) AS p2_raw,
          SUM(CASE WHEN lower(r.answer)='yes' THEN sd.p3 ELSE 0 END) AS p3_raw,
          SUM(CASE WHEN lower(r.answer)='yes' THEN sd.p4 ELSE 0 END) AS p4_raw
        FROM lca_response r
        JOIN lca_score_definitions sd ON sd.question_id = r.question_id
        WHERE r.run_id = :rid
    """), {"rid": run_id}).mappings().first()

def maxima():
    return db.session.execute(text("""
        SELECT SUM(p1) AS p1_max, SUM(p2) AS p2_max,
               SUM(p3) AS p3_max, SUM(p4) AS p4_max
        FROM lca_score_definitions
    """)).mappings().first()

# ---------- Persist materialized results (optional) ----------
def persist_results_row(run_id: int) -> None:
    t = totals_for_run(run_id)
    if not t:  # no answers yet
        t = {"p1_raw": 0, "p2_raw": 0, "p3_raw": 0, "p4_raw": 0}
    db.session.execute(text("""
        INSERT INTO lca_result (run_id, user_id, p1_raw, p2_raw, p3_raw, p4_raw, created_at)
        SELECT :rid, lr.user_id, :p1, :p2, :p3, :p4, datetime('now')
        FROM lca_run lr WHERE lr.id = :rid
        ON CONFLICT(run_id) DO UPDATE SET
          p1_raw = excluded.p1_raw,
          p2_raw = excluded.p2_raw,
          p3_raw = excluded.p3_raw,
          p4_raw = excluded.p4_raw,
          created_at = excluded.created_at
    """), {"rid": run_id,
           "p1": t["p1_raw"] or 0, "p2": t["p2_raw"] or 0,
           "p3": t["p3_raw"] or 0, "p4": t["p4_raw"] or 0})
    db.session.commit()
