from flask import flash, redirect, render_template, request, url_for, abort, session
from sqlalchemy.exc import SQLAlchemyError
from app.extensions import db
from app.admin import admin_bp
from app.models.reading import RdpLesson
from app.utils import reading_utils

@admin_bp.route("/<subject>/lessons", methods=["GET"], endpoint="lessons")
def admin_lessons(subject: str):
    subject = (subject or "").lower().strip()
    if subject != "reading":
        abort(404)

    lesson_rows = db.session.query(RdpLesson).order_by(RdpLesson.order.asc(), RdpLesson.id.asc()).all()
    email = (request.args.get("as") or session.get("email") or "").strip().lower()
    preview_ctx = reading_utils.dashboard_context(email)

    return render_template("admin/programs/reading/lessons.html", lessons=lesson_rows, preview_ctx=preview_ctx, subject=subject)

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
            return render_template("admin/programs/reading/new_lesson.html", subject=subject)

        next_order = db.session.query(db.func.coalesce(db.func.max(RdpLesson.order), 0)).scalar() + 1
        lesson = RdpLesson(title=title, caption=caption, order=next_order)
        if hasattr(lesson, "video_filename"):
            lesson.video_filename = video_filename

        db.session.add(lesson)
        db.session.commit()
        flash("Lesson created.", "success")
        return redirect(url_for("admin_bp.lessons", subject=subject))

    return render_template("admin/programs/reading/new_lesson.html", subject=subject)

@admin_bp.route("/<subject>/lesson/<int:lesson_id>/edit", methods=["GET", "POST"], endpoint="edit_lesson")
def edit_lesson(subject: str, lesson_id: int):
    subject = (subject or "").lower().strip()
    if subject != "reading":
        abort(404)

    lesson = db.session.get(RdpLesson, lesson_id)
    if not lesson:
        abort(404)

    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        caption = (request.form.get("caption") or "").strip()
        content = (request.form.get("content") or "").strip()
        video_filename = (request.form.get("video_filename") or "").strip()

        if not title:
            flash("Title is required.", "warning")
            return render_template("admin/programs/reading/edit_lesson.html", lesson=lesson, subject=subject)

        lesson.title = title
        if hasattr(lesson, "caption"):
            lesson.caption = caption
        if hasattr(lesson, "content"):
            lesson.content = content
        if hasattr(lesson, "video_filename"):
            lesson.video_filename = video_filename or ""

        try:
            db.session.commit()
            flash("Lesson updated.", "success")
            return redirect(url_for("admin_bp.lessons", subject=subject))
        except SQLAlchemyError as e:
            db.session.rollback()
            flash("Could not save changes.", "danger")
            return render_template("admin/programs/reading/edit_lesson.html", lesson=lesson, subject=subject)

    return render_template("admin/programs/reading/edit_lesson.html", lesson=lesson, subject=subject)

@admin_bp.route("/<subject>/reorder", methods=["GET"], endpoint="reorder")
def reorder(subject: str):
    subject = (subject or "").lower().strip()
    if subject != "reading":
        abort(404)
    lessons = db.session.query(RdpLesson.id, RdpLesson.title, RdpLesson.order).order_by(RdpLesson.order.asc(), RdpLesson.id.asc()).all()
    return render_template("admin/programs/reading/reorder.html", lessons=lessons, subject=subject)

from flask import jsonify
@admin_bp.route("/api/<subject>/lessons/reorder", methods=["POST"], endpoint="api_reorder_lessons")
def api_reorder_lessons(subject: str):
    subject = (subject or "").lower().strip()
    if subject != "reading":
        abort(404)
    payload = request.get_json(silent=True) or {}
    ids = payload.get("ids") or []
    if not ids or not all(isinstance(i, int) for i in ids):
        return jsonify(ok=False, error="Invalid ids"), 400

    for idx, lid in enumerate(ids, start=1):
        db.session.query(RdpLesson).filter_by(id=lid).update({"order": idx})
    db.session.commit()
    return jsonify(ok=True)

import os
from flask import send_from_directory, current_app

@admin_bp.route("/reading/audit-disk", methods=["GET"])
def audit_video_disk():
        
    dirs_to_check = [
        os.path.join(current_app.root_path, "static", "uploads", "reading_videos"),
        os.path.join(current_app.root_path, "static", "videos")
    ]
    
    found_files = []
    for d in dirs_to_check:
        if os.path.exists(d):
            for f in os.listdir(d):
                filepath = os.path.join(d, f)
                if os.path.isfile(filepath):
                    size_mb = round(os.path.getsize(filepath) / (1024 * 1024), 2)
                    # Create a safe relative path for downloading
                    rel_path = os.path.relpath(filepath, current_app.root_path)
                    found_files.append({"name": f, "size": size_mb, "path": rel_path, "dir": d})
                    
    # Generate a simple HTML response so we don't need a template file
    html = "<html><head><title>Disk Audit</title><style>body{font-family:sans-serif; padding:2rem;} table{border-collapse:collapse; width:100%;} th,td{border:1px solid #ccc; padding:0.5rem; text-align:left;} th{background:#eee;}</style></head><body>"
    html += "<h1>Render Server Disk Audit</h1>"
    html += "<p>These are the physical video files currently sitting on the live Render server.</p>"
    html += "<table><tr><th>Filename</th><th>Size (MB)</th><th>Location on Server</th><th>Action</th></tr>"
    for file in found_files:
        html += f"<tr><td>{file['name']}</td><td>{file['size']} MB</td><td>{file['dir']}</td>"
        html += f"<td><a href='/admin/reading/download-disk-file?path={file['path']}' style='background:#4f46e5;color:white;padding:5px 10px;text-decoration:none;border-radius:4px;'>Download</a></td></tr>"
    html += "</table><br><a href='/admin/reading/lessons'>&larr; Back to Admin</a></body></html>"
    return html

@admin_bp.route("/reading/download-disk-file", methods=["GET"])
def download_disk_file():
    
    file_path = request.args.get("path")
    if not file_path or ".." in file_path:
        abort(400)
        
    full_path = os.path.join(current_app.root_path, file_path)
    directory = os.path.dirname(full_path)
    filename = os.path.basename(full_path)
    
    return send_from_directory(directory, filename, as_attachment=True)
