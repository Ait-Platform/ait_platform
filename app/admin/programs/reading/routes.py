from flask import abort, current_app, flash, jsonify, redirect, render_template, request, session, url_for
from app.models.reading import RdpLesson
from app.extensions import db
from app.admin import admin_bp
from app.utils import reading_utils

@admin_bp.route("/reading/preview", methods=["GET"], endpoint="reading_preview")
def admin_reading_preview():
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return redirect(url_for("public_bp.welcome"))

    email = (request.args.get("as") or session.get("email") or "").strip().lower()
    ctx = reading_utils.dashboard_context(email)

    for item in ctx.get("items", []):
        item["can_start"] = True

    ctx["admin_preview"] = True
    return render_template("school_reading/learner_dashboard.html", **ctx)
