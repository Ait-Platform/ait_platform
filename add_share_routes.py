import re
content = open('app/program_healthcore/routes.py', 'r').read()

doctor_code = '''
# ---------------------------------------------------------
# DOCTOR ACCESS
# ---------------------------------------------------------
@healthcore_bp.route("/program/healthcore/share", methods=["GET"])
@login_required
@healthcore_onboarded_required
def share_dashboard():
    from app.models.healthcore import HcDoctorAccess
    shares = HcDoctorAccess.query.filter_by(user_id=current_user.id).order_by(HcDoctorAccess.created_at.desc()).all()
    return render_template("program_healthcore/share.html", shares=shares)

@healthcore_bp.route("/program/healthcore/share/add", methods=["POST"])
@login_required
@healthcore_onboarded_required
def create_share():
    from app.models.healthcore import HcDoctorAccess
    from datetime import datetime, timedelta
    import secrets
    
    doctor_email = request.form.get("doctor_email")
    doctor_name = request.form.get("doctor_name")
    days = request.form.get("days", type=int) or 7
    
    if not doctor_email:
        flash("Doctor's email is required.", "danger")
        return redirect(url_for("healthcore_bp.share_dashboard"))
        
    access_token = secrets.token_urlsafe(32)
    expires_at = datetime.utcnow() + timedelta(days=days)
    
    share = HcDoctorAccess(
        user_id=current_user.id,
        doctor_email=doctor_email,
        doctor_name=doctor_name,
        access_token=access_token,
        expires_at=expires_at
    )
    db.session.add(share)
    db.session.commit()
    
    flash("Secure sharing link generated!", "success")
    return redirect(url_for("healthcore_bp.share_dashboard"))

@healthcore_bp.route("/program/healthcore/share/revoke/<int:share_id>", methods=["POST"])
@login_required
def revoke_share(share_id):
    from app.models.healthcore import HcDoctorAccess
    share = HcDoctorAccess.query.filter_by(id=share_id, user_id=current_user.id).first_or_404()
    share.is_active = False
    db.session.commit()
    flash("Access revoked.", "info")
    return redirect(url_for("healthcore_bp.share_dashboard"))

@healthcore_bp.route("/healthcore/doctor/view/<token>")
def doctor_view(token):
    from app.models.healthcore import HcDoctorAccess, HcPatientProfile, HcRiskAssessment, HcLaboratory
    from datetime import datetime
    from app.models.auth import User
    
    share = HcDoctorAccess.query.filter_by(access_token=token, is_active=True).first_or_404()
    
    if datetime.utcnow() > share.expires_at:
        return "This secure link has expired.", 403
        
    patient = User.query.get(share.user_id)
    profile = HcPatientProfile.query.filter_by(user_id=share.user_id).first()
    risks = HcRiskAssessment.query.filter_by(user_id=share.user_id).order_by(HcRiskAssessment.calculated_date.desc()).limit(5).all()
    labs = HcLaboratory.query.filter_by(user_id=share.user_id).order_by(HcLaboratory.report_date.desc()).limit(15).all()
    
    return render_template("program_healthcore/doctor_view.html", share=share, patient=patient, profile=profile, risks=risks, labs=labs)
'''

if 'def share_dashboard():' not in content:
    content += doctor_code
    open('app/program_healthcore/routes.py', 'w').write(content)
