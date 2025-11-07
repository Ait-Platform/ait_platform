# app/school_loss/report_context.py
from typing import Any, Dict, Optional
from flask import url_for, render_template
from inspect import signature
from app.services.loss_report import build_report_ctx as _svc_build

def _call_service_build(run_id: int, user_id: Optional[int]):
    """Call build_report_ctx with the correct arity (1 or 2 params)."""
    try:
        params = signature(_svc_build).parameters
        if len(params) == 1:
            return _svc_build(run_id)
        return _svc_build(run_id, user_id)
    except TypeError:
        # Fallback if signature() disagrees in your env
        try:
            return _svc_build(run_id, user_id)
        except TypeError:
            return _svc_build(run_id)

def build_context(run_id: int, user_id: Optional[int], *, pdf_mode: bool = False) -> Dict[str, Any]:
    """Single source of truth for LOSS report context (no admin imports)."""
    res = _call_service_build(run_id, user_id)

    # normalize result to a dict 'ctx'
    if isinstance(res, dict):
        ctx, totals, row = res, None, None
    elif isinstance(res, tuple):
        if len(res) == 3:
            ctx, totals, row = res
        elif len(res) == 2:
            ctx, totals = res; row = None
        else:
            ctx, totals, row = res[0], None, None
    else:
        # last-resort: try attributes
        ctx  = getattr(res, "ctx", {}) if res is not None else {}
        totals = getattr(res, "totals", None)
        row  = getattr(res, "row", None)

    # --- header line fields for PDF ---
    user_name  = ctx.get("user_name")  or ctx.get("learner_name")  or "loss"
    user_email = ctx.get("user_email") or ctx.get("learner_email") or "loss@gmail.com"
    created_at = ctx.get("run_created_at") or ctx.get("created_at") \
                 or (row.get("created_at") if isinstance(row, dict) else getattr(row, "created_at", None))
    if hasattr(created_at, "strftime"):
        created_at_str = created_at.strftime("%Y-%m-%d %H:%M:%S")
    else:
        created_at_str = str(created_at) if created_at else ""

    # endpoints: ALL point to school_loss routes (not admin)
    ctx.update(
        run_id=run_id,
        user_id=user_id,
        pdf_mode=pdf_mode,
        header_user_name=user_name,
        header_user_email=user_email,
        header_run_created_at_str=created_at_str,
        screen_url=url_for("loss_bp.report", run_id=run_id, user_id=user_id),
        pdf_url=url_for("loss_bp.report_pdf_download", run_id=run_id, user_id=user_id),
        email_url=url_for("loss_bp.report_email_and_download", run_id=run_id),
    )
    return ctx

def render_report_html(run_id: int, user_id: int, *, pdf_mode: bool = False) -> str:
    ctx = build_context(run_id, user_id, pdf_mode=pdf_mode)
    return render_template("subject/loss/report.html", **ctx)

