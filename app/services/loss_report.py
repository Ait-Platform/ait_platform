from sqlalchemy import select, func
from app import db
from app.models.loss import LcaRun, LcaResult

def build_report_ctx(run_id: int) -> dict:
    run = db.session.get(LcaRun, run_id)

    row = db.session.execute(
        select(
            func.sum(LcaResult.phase_1).label("p1"),
            func.sum(LcaResult.phase_2).label("p2"),
            func.sum(LcaResult.phase_3).label("p3"),
            func.sum(LcaResult.phase_4).label("p4"),
            func.sum(LcaResult.total).label("tot"),
            func.count(LcaResult.id).label("rows"),
            func.min(LcaResult.created_at).label("first_at"),
            func.max(LcaResult.created_at).label("last_at"),
        ).where(LcaResult.run_id == run_id)
    ).one()

    status_display   = (run.status if run and getattr(run, "status", None) else "legacy")
    started_display  = (run.started_at if run and getattr(run, "started_at", None) else (row.first_at or "—"))
    finished_display = (run.finished_at if run and getattr(run, "finished_at", None) else "")

    return {
        "run": run,
        "run_id": run_id,
        "res_summary": row,
        "status_display": status_display,
        "started_display": started_display,
        "finished_display": finished_display,
    }
