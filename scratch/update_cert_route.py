import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace the certificate route
old_cert = '''@sace_bp.route("/sace/reading/certificate")
@login_required
def certificate():
    from datetime import datetime
    import uuid
    date_str = datetime.utcnow().strftime("%d %B %Y")
    session_id = str(uuid.uuid4())[:8].upper()
    return render_template("program_sace/post_test/certificate.html", date=date_str, session_id=session_id)'''

new_cert = '''@sace_bp.route("/sace/reading/certificate/email", methods=["POST"])
@login_required
def email_certificate():
    from datetime import datetime
    import uuid
    from app.subject_reading.routes import _generate_certificate_pdf, _email_certificate_pdf
    
    target_email = request.form.get("email")
    if not target_email:
        flash("Email address is required.", "error")
        return redirect(url_for("sace_bp.post_test_results"))
        
    cert_id = "AIT-WS-" + str(uuid.uuid4())[:8].upper()
    completed_at = datetime.utcnow()
    
    try:
        # Generate the standard PDF
        pdf_bytes = _generate_certificate_pdf(
            certificate_id=cert_id,
            learner_name=current_user.username,
            completed_at=completed_at,
            user_id=current_user.id
        )
        
        # Email it
        _email_certificate_pdf(
            to_email=target_email,
            learner_name=current_user.username,
            certificate_id=cert_id,
            pdf_bytes=pdf_bytes
        )
        
        flash(f"Certificate successfully emailed to {target_email}", "success")
    except Exception as e:
        current_app.logger.error(f"Failed to email SACE workshop certificate: {e}")
        flash("Failed to email certificate. Please try again later.", "error")
        
    return redirect(url_for("sace_bp.post_test_results"))'''

if old_cert in text:
    text = text.replace(old_cert, new_cert)
else:
    print("Could not find old_cert block")

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

