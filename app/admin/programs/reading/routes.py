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

@admin_bp.route("/reading/learners", methods=["GET"])
def reading_learners():
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return redirect(url_for("public_bp.welcome"))

    # Fetch all learners who have a reading enrollment
    rows = db.session.execute(
        db.text("""
            SELECT
                u.id,
                u.email,
                u.name,
                r.started_at,
                r.completed_at,
                CASE WHEN r.completed_at IS NOT NULL THEN 100 ELSE 0 END as progress_percent,
                '' as certificate_id
            FROM "user" u
            JOIN user_enrollment r ON u.id = r.user_id
            JOIN auth_subject s ON r.subject_id = s.id
            WHERE s.slug = 'reading'
            ORDER BY u.id DESC
        """)
    ).mappings().all()

    return render_template("admin/programs/reading/learners.html", learners=rows, subject="reading")

@admin_bp.route("/reading/learners/<int:user_id>/update-email", methods=["POST"])
def reading_update_email(user_id):
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    new_email = request.json.get("email", "").strip().lower()
    if not new_email:
        return jsonify({"success": False, "error": "Email is required"}), 400

    from app.models.auth import User
    user = db.session.query(User).get(user_id)
    if not user:
        return jsonify({"success": False, "error": "User not found"}), 404

    # Check for existing
    existing = db.session.query(User).filter_by(email=new_email).first()
    if existing and existing.id != user_id:
        return jsonify({"success": False, "error": "Email already in use"}), 400

    user.email = new_email
    db.session.commit()
    return jsonify({"success": True})

@admin_bp.route("/reading/learners/<int:user_id>/resend-certificate", methods=["POST"])
def reading_resend_certificate(user_id):
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    from app.models.auth import User
    user = db.session.query(User).get(user_id)
    if not user:
        return jsonify({"success": False, "error": "User not found"}), 404

    # fetch enrollment
    enr = db.session.execute(
        db.text("""
            SELECT r.*
            FROM user_enrollment r
            JOIN auth_subject s ON r.subject_id = s.id
            WHERE r.user_id = :uid AND s.slug = 'reading'
            LIMIT 1
        """),
        {"uid": user_id}
    ).mappings().first()

    if not enr:
        return jsonify({"success": False, "error": "User is not enrolled in Reading"}), 400

    from datetime import datetime, timezone
    completed_at = enr.completed_at
    if not completed_at:
        # Force complete the user if admin resends cert
        completed_at = datetime.now(timezone.utc)
        db.session.execute(
            db.text("UPDATE user_enrollment SET completed_at = :now, status = 'completed' WHERE id = :eid"),
            {"now": completed_at, "eid": enr.id}
        )
        db.session.commit()

    from app.subject_reading.routes import _generate_certificate_pdf, _email_certificate_pdf, _make_certificate_id
    
    # generate a consistent cert id
    cert_id = _make_certificate_id(user_id)
    learner_name = user.name or user.email

    pdf_bytes = _generate_certificate_pdf(
        certificate_id=cert_id,
        learner_name=learner_name,
        completed_at=completed_at,
    )

    if not pdf_bytes:
        return jsonify({"success": False, "error": "Failed to generate PDF"}), 500

    target_email = request.json.get("email", "").strip().lower() if request.is_json else ""
    if not target_email:
        target_email = user.email

    if target_email:
        _email_certificate_pdf(
            to_email=target_email,
            learner_name=learner_name,
            certificate_id=cert_id,
            pdf_bytes=pdf_bytes,
        )
        return jsonify({"success": True, "message": "Email sent successfully"})
    else:
        return jsonify({"success": False, "error": "No email address provided"}), 400

@admin_bp.route("/reading/learners/<int:user_id>/preview-certificate", methods=["GET"])
def reading_preview_certificate(user_id):
    if not (session.get("is_admin") or session.get("role") == "admin"):
        abort(403)

    from app.models.auth import User
    user = db.session.query(User).get(user_id)
    if not user:
        abort(404)

    # fetch enrollment
    enr = db.session.execute(
        db.text("""
            SELECT r.*
            FROM user_enrollment r
            JOIN auth_subject s ON r.subject_id = s.id
            WHERE r.user_id = :uid AND s.slug = 'reading'
            LIMIT 1
        """),
        {"uid": user_id}
    ).mappings().first()

    if not enr:
        abort(404)

    from datetime import datetime, timezone
    completed_at = enr.completed_at
    if not completed_at:
        # If not completed, use current time for the preview
        completed_at = datetime.now(timezone.utc)

    from app.subject_reading.routes import _generate_certificate_pdf, _make_certificate_id
    from flask import send_file
    import io
    
    cert_id = _make_certificate_id(user_id)
    learner_name = user.name or user.email

    pdf_bytes = _generate_certificate_pdf(
        certificate_id=cert_id,
        learner_name=learner_name,
        completed_at=completed_at,
    )

    if not pdf_bytes:
        abort(500)

    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"certificate_{user_id}.pdf"
    )
