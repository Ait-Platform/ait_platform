from flask import (
    Blueprint, current_app, render_template, 
    redirect, url_for, abort, request, session,
    jsonify, flash)
from app.models.loss import LcaResult
from app.utils import reading_utils
from app.utils.role_utils import is_admin  # reuse your helper
from app.extensions import db
from app.models.reading import RdpLesson
from .. import admin_bp
#admin_bp = Blueprint("admin_bp", __name__, url_prefix="/admin")
from sqlalchemy import select, func
# subjects you support in admin
ALLOWED_SUBJECTS = {"reading", "home", "loss", "billing"}  # extend as needed

@admin_bp.before_request
def _guard():
    if not is_admin():
        return redirect(url_for("public_bp.welcome"))



@admin_bp.route("/<subject>/", endpoint="subject_dashboard")
def subject_dashboard(subject: str):
    subject = (subject or "").lower().strip()
    if subject not in ALLOWED_SUBJECTS:
        abort(404)
    return render_template(f"admin/{subject}/dashboard.html", subject=subject)

# --- Lessons list (simple) ---
@admin_bp.route("/<subject>/lessons", endpoint="lessons")
def lessons(subject: str):
    if subject != "reading":
        abort(404)
    lessons = db.session.query(RdpLesson).order_by(RdpLesson.order.asc(), RdpLesson.id.asc()).all()
    return render_template("admin/reading/lessons.html", lessons=lessons, subject=subject)

# --- Reorder UI ---
@admin_bp.route("/<subject>/reorder", methods=["GET"], endpoint="reorder")
def reorder(subject: str):
    if subject != "reading":
        abort(404)
    lessons = (
        db.session.query(RdpLesson.id, RdpLesson.title, RdpLesson.order)
        .order_by(RdpLesson.order.asc(), RdpLesson.id.asc())
        .all()
    )
    return render_template("admin/reading/reorder.html", lessons=lessons, subject=subject)

# --- Save new order (JSON) ---
@admin_bp.route("/api/<subject>/lessons/reorder", methods=["POST"], endpoint="api_reorder_lessons")
def api_reorder_lessons(subject: str):
    if subject != "reading":
        abort(404)
    payload = request.get_json(silent=True) or {}
    ids = payload.get("ids") or []
    if not ids or not all(isinstance(i, int) for i in ids):
        return jsonify(ok=False, error="Invalid ids"), 400

    # Only reorder the provided ids; we expect all lessons are present in the UI.
    for idx, lid in enumerate(ids, start=1):
        db.session.query(RdpLesson).filter_by(id=lid).update({"order": idx})
    db.session.commit()
    return jsonify(ok=True)

# ... existing admin_bp and routes ...

@admin_bp.route("/<subject>/lessons/new", methods=["GET", "POST"], endpoint="new_lesson")
def new_lesson(subject: str):
    subject = (subject or "").lower().strip()
    if subject != "reading":
        abort(404)

    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        caption = (request.form.get("caption") or "").strip()
        video_filename = (request.form.get("video_filename") or "").strip()

        if not title:
            flash("Title is required.", "warning")
            return render_template(
                "admin/reading/new_lesson.html",
                subject=subject, title=title,
                caption=caption, video_filename=video_filename
            )

        # Ensure NOT NULL at DB level
        video_filename = video_filename or ""

        # Validate extension (optional but helpful)
        allowed_ext = {"mp4", "webm", "ogg", "png", "jpg", "jpeg", "gif", "webp", "svg", "pdf"}
        if video_filename:
            ext = video_filename.rsplit(".", 1)[-1].lower() if "." in video_filename else ""
            if ext not in allowed_ext:
                flash(
                    f"Unsupported file type '.{ext}'. Allowed: {', '.join(sorted(allowed_ext))}.",
                    "warning"
                )
                return render_template(
                    "admin/reading/new_lesson.html",
                    subject=subject, title=title,
                    caption=caption, video_filename=video_filename
                )

        # Warn (non-blocking) if file not present under /static/
        # If no folder given, we check /static/videos/<file> (template will also default there).
        try:
            import os
            root = current_app.static_folder  # absolute path to /static
            candidate = video_filename
            if candidate and ("/" not in candidate and "\\" not in candidate):
                candidate = os.path.join("videos", candidate)  # default folder only for the check

            if candidate:
                # Normalize and ensure the path stays within /static (safety)
                fullpath = os.path.normpath(os.path.join(root, candidate))
                inside_static = os.path.commonpath([root, fullpath]) == os.path.normpath(root)
                if inside_static and not os.path.exists(fullpath):
                    flash(
                        f"Warning: '{candidate}' not found under /static/. "
                        "The lesson will save, but media won't render until the file is placed there.",
                        "warning"
                    )
        except Exception:
            # Non-fatal; just skip the existence hint
            pass

        # Append to end of sequence
        next_order = db.session.query(db.func.coalesce(db.func.max(RdpLesson.order), 0)).scalar() + 1

        lesson = RdpLesson(title=title, caption=caption, order=next_order)

        # Reading uses in-house media only
        if hasattr(lesson, "video_filename"):
            # Store exactly what the admin typed (template knows how to resolve it)
            lesson.video_filename = video_filename   # may be "" (NOT NULL safe)

        db.session.add(lesson)
        db.session.commit()
        flash("Lesson created.", "success")
        return redirect(url_for("admin_bp.lessons", subject="reading"))

    # GET
    return render_template("admin/reading/new_lesson.html", subject=subject)

# app/admin/reading/routes.py
@admin_bp.route("/reading/preview", methods=["GET"], endpoint="reading_preview")
def admin_reading_preview():
    # Admin gate
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return redirect(url_for("public_bp.welcome"))

    # Optional: preview as a specific learner ?as=email
    email = (request.args.get("as") or session.get("email") or "").strip().lower()

    # Single source of truth for learner dashboard data
    ctx = reading_utils.dashboard_context(email)

    # In preview we want all cards clickable (no gating)
    for item in ctx.get("items", []):
        item["can_start"] = True

    # Let the template show a "Back to Admin" link/banner if you want
    ctx["admin_preview"] = True

    return render_template("school_reading/learner_dashboard.html", **ctx)



# app/admin/routes.py (or wherever your admin_bp routes are)
# app/admin/routes.py
from flask import render_template, current_app
from . import admin_bp

# ---- Tiles dashboard view ----

# app/admin/routes.py
from flask import request, session, redirect, url_for

@admin_bp.before_request
def _admin_gate():
    # 1) Only block non-admins
    if not session.get("is_admin"):
        return redirect(url_for("public_bp.welcome"))

    # 2) Never redirect the main admin dashboard (tiles)
    if request.path.rstrip("/") == "/admin":
        return None
    if request.endpoint in ("admin_bp.admin_dashboard", "admin_bp.index"):
        return None

    # 3) Allow everything else (billing, loss, reading, etc.)
    return None



def _admins_only():
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return redirect(url_for("public_bp.welcome"))  # not login
    return None

# /admin/ → templates/admin/admin_dashboard.html






from . import admin_bp
from flask import render_template, session, redirect, url_for

# Simple auth gate: only block non-admins
@admin_bp.before_request
def _admin_only():
    if not session.get("is_admin"):
        return redirect(url_for("public_bp.welcome"))

# --- define the view function ONCE ---
def _admin_dashboard_view():
    # Renders your main admin dashboard UI
    return render_template("admin/index.html", subjects=sorted(ALLOWED_SUBJECTS))

# --- bind BOTH endpoints to the same function (AFTER it's defined) ---
admin_bp.add_url_rule("/", endpoint="index",            view_func=_admin_dashboard_view)
admin_bp.add_url_rule("/", endpoint="admin_dashboard",  view_func=_admin_dashboard_view)

# app/blueprints/admin/loss_admin.py


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
