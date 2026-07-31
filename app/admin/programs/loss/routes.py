from flask import render_template
from sqlalchemy import func, select

from app.models.loss import LcaResult
from app.extensions import db
from app.admin import admin_bp

@admin_bp.route("/loss/runs", methods=["GET"], endpoint="loss_runs_selector")
def loss_runs_selector():
    rows = db.session.execute(
        select(
            LcaResult.run_id.label("run_id"),
            func.max(LcaResult.created_at).label("last_at"),
            func.count().label("answers")
        )
        .where(LcaResult.run_id.isnot(None))
        .group_by(LcaResult.run_id)
        .order_by(func.max(LcaResult.created_at).desc())
    ).all()
    return render_template("admin/loss/runs_selector.html", runs=rows)
