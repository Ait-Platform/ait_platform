from datetime import datetime
from sqlalchemy import text
from app import db

def loss_create_run(user_id:int, subject:str="LOSS")->int:
    row = db.session.execute(text("""
        INSERT INTO lca_run (user_id, subject, status, started_at)
        VALUES (:uid, :subj, 'in_progress', :ts)
        RETURNING id
    """), {"uid": user_id, "subj": subject, "ts": datetime.utcnow().isoformat(timespec="seconds")}).first()
    db.session.commit()
    return int(row[0])

def loss_finish_run(run_id:int):
    db.session.execute(text("""
        UPDATE lca_run SET status='finished', finished_at=:ts WHERE id=:rid
    """), {"rid": run_id, "ts": datetime.utcnow().isoformat(timespec="seconds")})
    db.session.commit()

def loss_latest_run_id(user_id:int):
    r = db.session.execute(text("""
        SELECT id FROM lca_run
        WHERE user_id=:uid
        ORDER BY COALESCE(finished_at, started_at) DESC
        LIMIT 1
    """), {"uid": user_id}).first()
    return int(r[0]) if r else None

def loss_list_runs(user_id:int, limit:int=50):
    rows = db.session.execute(text("""
        SELECT id, status, subject, started_at, finished_at
        FROM lca_run
        WHERE user_id=:uid
        ORDER BY COALESCE(finished_at, started_at) DESC
        LIMIT :lim
    """), {"uid": user_id, "lim": limit}).mappings().all()
    return rows
