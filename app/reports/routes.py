# reports/routes.py
from flask import Blueprint, ctx, current_app, flash, redirect, render_template, request, send_file, abort, url_for
from itsdangerous import URLSafeSerializer, BadData
import io
from app.subject_loss.report_context_adapter import build_learner_report_ctx
from app.utils.pdf_render import html_to_pdf_bytes


reports_bp = Blueprint("reports_bp", __name__)

@reports_bp.route("/download/<token>")
def download_report(token):
    # Match the signer used by LOSS report links; retain explicit overrides.
    serializer = current_app.config.get("REPORT_SERIALIZER")
    if serializer is None:
        serializer = URLSafeSerializer(current_app.secret_key, salt="pdf-report")
    try:
        data = serializer.loads(token)
    except BadData:
        abort(403)

    if not isinstance(data, dict):
        abort(403)
    run_id = data.get("run_id")
    user_id = data.get("user_id")
    if any(type(value) is not int or value <= 0 for value in (run_id, user_id)):
        abort(403)

    ctx = build_learner_report_ctx(run_id, user_id)
    if not ctx:
        abort(404)
    ctx["pdf_mode"] = True

    # Render the lean LOSS PDF template (make sure it includes _styles_base.html)
    html = render_template("subject/loss/report_pdf_flop.html", **ctx)

    try:
        pdf_bytes = html_to_pdf_bytes(html, base_url=request.host_url)
    except Exception as e:
        current_app.logger.exception(f"PDF generation failed: {e}")
        flash("PDF engine not configured yet. Showing web summary instead.", "info")
        return redirect(url_for("loss_bp.results_hub", run_id=run_id))

    # Return raw PDF bytes with headers (simpler than send_file)
    return (pdf_bytes, 200, {
        "Content-Type": "application/pdf",
        "Content-Disposition": f'inline; filename="loss-result-run-{run_id}.pdf"'
    })

