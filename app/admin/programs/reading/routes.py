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
                r.progress_percent,
                r.certificate_id
            FROM users u
            JOIN rdp_enrollment r ON u.id = r.user_id
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
        db.text("SELECT * FROM rdp_enrollment WHERE user_id = :uid LIMIT 1"),
        {"uid": user_id}
    ).mappings().first()

    if not enr or not enr.completed_at:
        return jsonify({"success": False, "error": "User has not completed the course"}), 400

    from app.subject_reading.routes import _generate_certificate_pdf, _email_certificate_pdf, _make_certificate_id
    
    cert_id = enr.certificate_id or _make_certificate_id(user_id)
    learner_name = user.name or user.email

    pdf_bytes = _generate_certificate_pdf(
        certificate_id=cert_id,
        learner_name=learner_name,
        completed_at=enr.completed_at,
    )

    if not pdf_bytes:
        return jsonify({"success": False, "error": "Failed to generate PDF"}), 500

    if user.email:
        _email_certificate_pdf(
            to_email=user.email,
            learner_name=learner_name,
            certificate_id=cert_id,
            pdf_bytes=pdf_bytes,
        )
        return jsonify({"success": True, "message": "Email sent successfully"})
    else:
        return jsonify({"success": False, "error": "User has no email"}), 400
