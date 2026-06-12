# reports/routes.py
from flask import Blueprint, ctx, current_app, flash, redirect, render_template, request, send_file, abort, url_for
from itsdangerous import URLSafeSerializer
import io
from app.subject_loss.report_context_adapter import build_learner_report_ctx
from app.utils.pdf_render import html_to_pdf_bytes


reports_bp = Blueprint("reports_bp", __name__)

@reports_bp.route("/download/<token>")
def download_report(token):
    serializer = current_app.config["REPORT_SERIALIZER"]
    try:
        data = serializer.loads(token)
    except Exception as e:
        current_app.logger.warning(f"Invalid report token: {e}")
        abort(403)

    run_id = data.get("run_id")
    user_id = data.get("user_id")

    ctx = build_learner_report_ctx(run_id, user_id) or {}
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

