# app/school_loss/report_context_adapter.py
from __future__ import annotations
from flask import current_app
# ← use the SAME builder that already works for admin
from app.admin.loss.routes import _build_context, _phase_scores_from_blocks
from app.subject_loss.charts import phase_scores_bar
#from app.subject.loss.charts import phase_scores_bar
#from app.subject.loss.charts import phase_scores_bar, _phase_scores_from_blocks
#from app.loss.report_shared import build_context as _build_context

def build_learner_report_ctx(run_id: int, user_id: int | None):
    ctx, _, _row = _build_context(run_id, user_id)
    if not ctx:
        return None

    # Header line for _header.html
    ctx["run_id"]        = run_id
    ctx["user_id"]       = user_id
    ctx["learner_name"]  = ctx.get("learner_name") or ctx.get("user_name") or ""
    ctx["learner_email"] = ctx.get("learner_email") or ctx.get("user_email") or ""
    ctx["taken_at"]      = ctx.get("taken_at_str") or ctx.get("taken_at") or ""

    # Vertical "Phase Graph"
    try:
        scores = _phase_scores_from_blocks(ctx)
        ctx["phase_scores_pct"] = scores
        data_uri, _ = phase_scores_bar(scores)


        ctx["phase_scores_chart_src"] = data_uri
    except Exception as e:
        current_app.logger.exception("phase_scores_bar failed: %s", e)
        ctx["phase_scores_pct"] = None
        ctx["phase_scores_chart_src"] = None

    ctx["viewer_is_admin"] = False
    ctx["pdf_mode"] = False
    return ctx

def _phase_scores_from_blocks(blocks):
    """Sum p1..p4 fields from phase blocks (works for dicts or objects)."""
    totals = {"p1": 0, "p2": 0, "p3": 0, "p4": 0}
    for b in (blocks or []):
        for k in totals:
            v = (b.get(k) if isinstance(b, dict) else getattr(b, k, 0)) or 0
            try:
                totals[k] += int(v)
            except Exception:
                pass
    return totals
