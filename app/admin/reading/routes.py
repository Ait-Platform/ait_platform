from flask import (
    Blueprint, current_app, render_template, 
    redirect, url_for, abort, request, 
    jsonify, flash, session)
from app.utils import reading_utils
#from app.utils.role_utils import is_admin  # reuse your helper
from app.extensions import db
from app.models.reading import RdpLesson,RdpLearnerProgress
from app.utils.roles import is_admin
from .. import admin_bp

from sqlalchemy.exc import SQLAlchemyError
#admin_bp = Blueprint("admin_bp", __name__, url_prefix="/admin")

# subjects you support in admin
ALLOWED_SUBJECTS = {"reading", "home", "loss", "billing"}  # extend as needed

@admin_bp.before_request
def _guard():
    if not is_admin():
        return redirect(url_for("public_bp.welcome"))

@admin_bp.route("/")   # ← no endpoint="index"
def admin_home():      # ← unique name; endpoint becomes "admin_bp.admin_home"
    return render_template("admin/index.html", subjects=sorted(ALLOWED_SUBJECTS))

@admin_bp.route("/<subject>/", endpoint="subject_dashboard")
def subject_dashboard(subject: str):
    subject = (subject or "").lower().strip()
    if subject not in ALLOWED_SUBJECTS:
        abort(404)
    return render_template(f"admin/{subject}/dashboard.html", subject=subject)

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

@admin_bp.route("/lessons/reorder", endpoint="reorder_lessons_page")
def reorder_lessons_page():
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return redirect(url_for("public_bp.welcome"))

    lessons = (
        db.session.query(RdpLesson)
        .order_by(RdpLesson.order.asc(), RdpLesson.id.asc())
        .all()
    )
    return render_template("admin/reading/reorder.html", lessons=lessons, subject="reading")

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

@admin_bp.route("/reading/", endpoint="reading_admin_home")
def reading_home():
    return render_template("admin/reading/index.html")

@admin_bp.route("/reading/dashboard", endpoint="reading_dashboard")
def admin_reading_dashboard():
    # Require admin
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return redirect(url_for("public_bp.welcome"))

    # Optional: preview another user with ?as=<email>
    email = (request.args.get("as") or session.get("email") or "").strip().lower()

    from app.utils import reading_utils
    ctx = reading_utils.dashboard_context(email)

    return render_template(
        "school_reading/dashboard.html",   # ← same template
        admin_preview=True,                # ← admin mode
        **ctx
    )

# app/admin/reading/routes.py

# --- Lessons list (simple) ---
# Canonical: subject-aware lessons page (with live preview on the right)
@admin_bp.route("/<subject>/lessons", methods=["GET"], endpoint="lessons")
def admin_lessons(subject: str):
    subject = (subject or "").lower().strip()
    if subject != "reading":
        abort(404)

    # gate – admin only
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return redirect(url_for("public_bp.welcome"))

    # left: raw lessons list
    lesson_rows = (
        db.session.query(RdpLesson)
        .order_by(RdpLesson.order.asc(), RdpLesson.id.asc())
        .all()
    )

    # right: live learner preview
    email = (request.args.get("as") or session.get("email") or "").strip().lower()
    preview_ctx = reading_utils.dashboard_context(email)  # returns items, learner_name, etc.

    return render_template(
        "admin/reading/lessons.html",
        lessons=lesson_rows,
        preview_ctx=preview_ctx,
        subject=subject,
    )

def _admins_only():
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return redirect(url_for("public_bp.welcome"))

# --- Manage (list & edit) ---
@admin_bp.route("/admin/reading/lessons")
def reading_lessons_manage():
    # You already do something here to fetch lessons
    lessons = RdpLesson.query.all()   # or whatever you had before

    # Provide a safe preview context so template never breaks
    preview_ctx = {
        "learner_name": "Preview Learner"
    }

    return render_template(
        "admin/reading/lessons_manage.html",
        lessons=lessons,
        preview_ctx=preview_ctx
    )

# --- Preview (table-only, no learner blueprint links) ---

# --- Preview a single lesson (admin-only, inside admin) ---

# new same endpoint
@admin_bp.route("/reading/lesson/<int:lesson_id>/preview", endpoint="preview_lesson")
def preview_lesson(lesson_id: int):
    gate = _admins_only()
    if gate:
        return gate

    lesson = db.session.get(RdpLesson, lesson_id)
    if not lesson:
        abort(404)

    media = (getattr(lesson, "video_filename", "") or "").strip()

    # If no folder supplied, pick a sensible default based on extension
    if media and ("/" not in media and "\\" not in media):
        ext = media.rsplit(".", 1)[-1].lower() if "." in media else ""
        video_exts = {"mp4", "webm", "ogg"}
        image_exts = {"png", "jpg", "jpeg", "gif", "webp", "svg"}
        if ext in video_exts:
            media = f"videos/{media}"
        elif ext in image_exts:
            media = f"images/{media}"
        elif ext == "pdf":
            media = f"docs/{media}"
        else:
            # leave as-is if unknown; template will show 'unsupported' message
            pass

    return render_template(
        "admin/reading/lesson_preview.html",
        lesson=lesson,
        media_relpath=media,
    )

@admin_bp.route("/<subject>/lesson/<int:lesson_id>/edit", methods=["GET", "POST"], endpoint="edit_lesson")
def edit_lesson(subject: str, lesson_id: int):
    # gate + subject guard (same pattern you use elsewhere)
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return redirect(url_for("public_bp.welcome"))
    subject = (subject or "").strip().lower()
    if subject != "reading":
        abort(404)

    # fetch lesson
    lesson = db.session.get(RdpLesson, lesson_id)
    if not lesson:
        abort(404)

    if request.method == "POST":
        # --- read form ---
        title          = (request.form.get("title") or "").strip()
        caption        = (request.form.get("caption") or "").strip()
        content        = (request.form.get("content") or "").strip()
        video_filename = (request.form.get("video_filename") or "").strip()

        # --- validate ---
        if not title:
            flash("Title is required.", "warning")
            return render_template("admin/reading/edit_lesson.html", lesson=lesson)

        # Optional media extension check (mirrors your new_lesson)
        if video_filename:
            allowed_ext = {"mp4","webm","ogg","png","jpg","jpeg","gif","webp","svg","pdf"}
            ext = video_filename.rsplit(".", 1)[-1].lower() if "." in video_filename else ""
            if ext not in allowed_ext:
                flash(f"Unsupported file type '.{ext}'. Allowed: {', '.join(sorted(allowed_ext))}.",
                      "warning")
                return render_template("admin/reading/edit_lesson.html", lesson=lesson)

        # --- apply changes safely (only if attrs exist) ---
        lesson.title   = title
        if hasattr(lesson, "caption"):
            lesson.caption = caption
        if hasattr(lesson, "content"):
            lesson.content = content
        if hasattr(lesson, "video_filename"):
            # keep NOT NULL discipline you used in new_lesson
            lesson.video_filename = video_filename or ""

        # --- persist ---
        try:
            db.session.commit()
            flash("Lesson updated.", "success")
            return redirect(url_for("admin_bp.reading_lessons"))
        except SQLAlchemyError as e:
            db.session.rollback()
            flash("Could not save changes. Please try again.", "danger")
            # If you want the exact error for debugging uncomment next line:
            # flash(str(e), "danger")
            return render_template("admin/reading/edit_lesson.html", lesson=lesson)

    # GET -> render form
    return render_template("admin/reading/edit_lesson.html", lesson=lesson)

@admin_bp.route("/reading/lessons")
def reading_lessons():
    gate = _admins_only()
    if gate:
        return gate

    lessons = reading_utils._lessons()  # handles ordering correctly
    return render_template("admin/reading/lessons.html", lessons=lessons)

# app/admin/reading/routes.py

@admin_bp.route("/reading/lessons/preview", endpoint="reading_lessons_preview")
def reading_lessons_preview():
    # Legacy route: just bounce back to Manage Lessons
    return redirect(url_for("admin_bp.reading_lessons"))


@admin_bp.route("/reading/preview", endpoint="reading_preview")
def reading_preview_alias():
    # Reuse the existing handler to avoid drift
    return reading_lessons_preview()


