from flask import abort, current_app, flash, jsonify, redirect, render_template, request, session, url_for
from app.extensions import db
from app.admin import admin_bp

@admin_bp.route("/home/learners", methods=["GET"])
def home_learners():
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return redirect(url_for("public_bp.welcome"))

    # Fetch all learners who have a home enrollment
    rows = db.session.execute(
        db.text("""
            SELECT
                u.id,
                u.email,
                u.name,
                r.started_at,
                r.completed_at,
                CASE WHEN r.completed_at IS NOT NULL THEN 100 ELSE 0 END as progress_percent
            FROM "user" u
            JOIN user_enrollment r ON u.id = r.user_id
            JOIN auth_subject s ON r.subject_id = s.id
            WHERE s.slug = 'home'
            ORDER BY u.id DESC
        """)
    ).mappings().all()

    return render_template("admin/programs/home/learners.html", learners=rows, subject="home")

@admin_bp.route("/home/learners/<int:user_id>/update-email", methods=["POST"])
def home_update_email(user_id):
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

@admin_bp.route("/home/learners/<int:user_id>/resend-certificate", methods=["POST"])
def home_resend_certificate(user_id):
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    from app.models.auth import User
    from app.models.home import HomeFinalAssessment
    user = db.session.query(User).get(user_id)
    if not user:
        return jsonify({"success": False, "error": "User not found"}), 404

    # fetch latest passed assessment
    assessment = db.session.query(HomeFinalAssessment).filter_by(user_id=user_id, passed=True).order_by(HomeFinalAssessment.id.desc()).first()
    
    if not assessment:
        return jsonify({"success": False, "error": "User has not passed the HOME final exam"}), 400

    from app.subject_home.routes import _generate_home_certificate_pdf, send_pdf_email
    
    pdf_bytes = _generate_home_certificate_pdf(assessment)

    if not pdf_bytes:
        return jsonify({"success": False, "error": "Failed to generate PDF"}), 500

    target_email = request.json.get("email", "").strip().lower() if request.is_json else ""
    if not target_email:
        target_email = user.email

    if target_email:
        send_pdf_email(
            to_email=target_email,
            subject="Your HOME Certificate & Diagnostic Report",
            body_text="Congratulations on passing the HOME Programme! Please find your official Certificate and Diagnostic Report attached.",
            pdf_bytes=pdf_bytes,
            filename="HOME_Certificate_and_Report.pdf"
        )
        return jsonify({"success": True, "message": "Email sent successfully"})
    else:
        return jsonify({"success": False, "error": "No email address provided"}), 400

@admin_bp.route("/home/learners/<int:user_id>/preview-certificate", methods=["GET"])
def home_preview_certificate(user_id):
    if not (session.get("is_admin") or session.get("role") == "admin"):
        abort(403)

    from app.models.auth import User
    from app.models.home import HomeFinalAssessment
    user = db.session.query(User).get(user_id)
    if not user:
        abort(404)

    # fetch latest passed assessment
    assessment = db.session.query(HomeFinalAssessment).filter_by(user_id=user_id, passed=True).order_by(HomeFinalAssessment.id.desc()).first()

    if not assessment:
        flash("User has not passed the HOME final exam yet.", "warning")
        return redirect(url_for('admin_bp.home_learners'))

    from app.subject_home.routes import _generate_home_certificate_pdf
    from flask import send_file
    import io
    
    pdf_bytes = _generate_home_certificate_pdf(assessment)

    if not pdf_bytes:
        abort(500)

    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=False, # Show in browser
        download_name=f"HOME_certificate_{user_id}.pdf"
    )
