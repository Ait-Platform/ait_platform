from flask import render_template, request, redirect, url_for, flash, abort
from flask_login import current_user, login_required
from app.extensions import db
from datetime import datetime, timedelta
from app.models.auth import User
from app.models.practice_crm import CrmPractice, CrmPracticeUser, CrmEnquiry, CrmAuditLog
from . import practice_crm_bp

def log_audit(enquiry_id, user_id, action):
    audit = CrmAuditLog(
        enquiry_id=enquiry_id,
        user_id=user_id,
        action=action,
        timestamp=datetime.utcnow()
    )
    db.session.add(audit)

@practice_crm_bp.route("/about")
def about():
    """Welcome / Sales page for Practice CRM"""
    return render_template("program_practice_crm/about.html")

@practice_crm_bp.route("/migrate_db")
def migrate_db():
    from sqlalchemy import text
    try:
        db.session.execute(text("ALTER TABLE crm_enquiry ADD COLUMN medical_aid_plan VARCHAR(150);"))
        db.session.commit()
        return "Migration successful!"
    except Exception as e:
        return f"Error: {str(e)}"

@practice_crm_bp.route("/dashboard")
@login_required
def dashboard():
    """Owner dashboard: metrics and exception reports"""
    
    from app.models.auth import UserEnrollment, AuthSubject
    
    # Check if the user is a receptionist by checking their enrollment
    crm_subj = AuthSubject.query.filter_by(slug='practice_crm').first()
    if crm_subj:
        enr = UserEnrollment.query.filter_by(user_id=current_user.id, subject_id=crm_subj.id).first()
        if enr and enr.status == 'receptionist':
            # Receptionists do not get access to the exception report
            # Check if they have a practice yet
            pu = CrmPracticeUser.query.filter_by(user_id=current_user.id).first()
            if not pu:
                flash("Your Practice Owner has not added you to their staff list yet. Please ask them to add your email address.", "warning")
                return redirect(url_for('public_bp.welcome'))
            return redirect(url_for('practice_crm_bp.pipeline'))

    # Find the practice for this owner
    practice = CrmPractice.query.filter_by(owner_id=current_user.id).first()
    if not practice:
        # Create default practice if none exists
        practice = CrmPractice(owner_id=current_user.id, name=f"{current_user.name or 'My'} Practice")
        db.session.add(practice)
        db.session.commit()
    
    # Exception Report Logic
    today = datetime.utcnow().date()
    yesterday = datetime.utcnow() - timedelta(days=1)
    
    enquiries = CrmEnquiry.query.filter_by(practice_id=practice.id).all()
    
    # Dashboard metrics (General Pipeline stats for owner)
    open_enquiries = [e for e in enquiries if e.status not in ('Booked', 'Not Booked')]
    
    # EXCEPTIONS:
    # 1. Enquiries with no follow-up after 24 hours (Stuck in New or Verification Pending)
    no_followup_24h = [e for e in enquiries if e.status in ('New', 'Verification Pending') and e.updated_at < yesterday]
    
    # 2. Verified patients not booked
    verified_not_booked = [e for e in enquiries if e.status in ('Verified', 'Appointment Offered')]
    
    # 3. Enquiries closed without booking
    closed_no_booking = [e for e in enquiries if e.status == 'Not Booked']
    
    # 4. Receptionist activity report
    from sqlalchemy import func
    activity = db.session.query(
        User.name,
        func.count(CrmEnquiry.id).label('total_created'),
        func.sum(db.case((CrmEnquiry.status == 'Booked', 1), else_=0)).label('total_booked')
    ).join(CrmEnquiry, User.id == CrmEnquiry.created_by_id) \
     .filter(CrmEnquiry.practice_id == practice.id) \
     .group_by(User.name).all()
    
    return render_template(
        "program_practice_crm/dashboard.html",
        practice=practice,
        open_count=len(open_enquiries),
        no_followup_24h=no_followup_24h,
        verified_not_booked=verified_not_booked,
        closed_no_booking=closed_no_booking,
        activity=activity
    )

@practice_crm_bp.route("/settings", methods=["GET", "POST"])
@login_required
def practice_settings():
    """Owner dashboard: update practice biodata"""
    practice = CrmPractice.query.filter_by(owner_id=current_user.id).first()
    if not practice:
        flash("Please visit the dashboard first to initialize your practice.", "warning")
        return redirect(url_for('practice_crm_bp.dashboard'))
        
    if request.method == "POST":
        practice.name = request.form.get("name", "").strip()
        practice.email = request.form.get("email", "").strip()
        practice.phone = request.form.get("phone", "").strip()
        practice.address = request.form.get("address", "").strip()
        practice.dentist_details = request.form.get("dentist_details", "").strip()
        practice.operating_hours = request.form.get("operating_hours", "").strip()
        practice.slot_settings = request.form.get("slot_settings", "").strip()
        
        db.session.commit()
        flash("Practice settings updated successfully.", "success")
        return redirect(url_for("practice_crm_bp.practice_settings"))
        
    return render_template("program_practice_crm/settings.html", practice=practice)

@practice_crm_bp.route("/staff", methods=["GET", "POST"])
@login_required
def staff():
    """Manage receptionists"""
    practice = CrmPractice.query.filter_by(owner_id=current_user.id).first()
    if not practice:
        flash("Please visit the dashboard first to initialize your practice.", "warning")
        return redirect(url_for('practice_crm_bp.dashboard'))
        
    if request.method == "POST":
        email = request.form.get('email', '').strip()
        user = User.query.filter_by(email=email).first()
        if not user:
            flash(f"User with email {email} not found. They must create an account first.", "error")
        else:
            # Check if already in practice
            existing = CrmPracticeUser.query.filter_by(practice_id=practice.id, user_id=user.id).first()
            if existing:
                flash(f"{user.name} is already a receptionist.", "info")
            else:
                pu = CrmPracticeUser(practice_id=practice.id, user_id=user.id, role='receptionist')
                db.session.add(pu)
                db.session.commit()
                flash(f"Added {user.name} as a receptionist.", "success")
        return redirect(url_for('practice_crm_bp.staff'))
        
    from app.models.auth import UserEnrollment, AuthSubject
    crm_subj = AuthSubject.query.filter_by(slug='practice_crm').first()
    available_receptionists = []
    if crm_subj:
        # Get users enrolled as receptionist who are NOT currently in this practice
        subquery = db.session.query(CrmPracticeUser.user_id).filter(CrmPracticeUser.practice_id == practice.id)
        available_receptionists = db.session.query(User).join(UserEnrollment).filter(
            UserEnrollment.subject_id == crm_subj.id,
            UserEnrollment.status == 'receptionist',
            ~User.id.in_(subquery)
        ).all()
        
    receptionists = db.session.query(CrmPracticeUser, User).join(User, CrmPracticeUser.user_id == User.id).filter(CrmPracticeUser.practice_id == practice.id).all()
    return render_template("program_practice_crm/staff.html", practice=practice, receptionists=receptionists, available_receptionists=available_receptionists)

@practice_crm_bp.route("/staff/<int:pu_id>/edit", methods=["POST"])
@login_required
def staff_edit(pu_id):
    """Owner edits a receptionist's biodata"""
    practice = CrmPractice.query.filter_by(owner_id=current_user.id).first()
    if not practice:
        flash("Unauthorized.", "error")
        return redirect(url_for('practice_crm_bp.dashboard'))
        
    pu = CrmPracticeUser.query.filter_by(id=pu_id, practice_id=practice.id).first()
    if not pu:
        flash("Receptionist not found.", "error")
        return redirect(url_for('practice_crm_bp.staff'))
        
    user = User.query.get(pu.user_id)
    if user:
        user.name = request.form.get("name", "").strip()
        pu.phone = request.form.get("phone", "").strip()
        db.session.commit()
        flash("Receptionist details updated.", "success")
        
    return redirect(url_for('practice_crm_bp.staff'))

@practice_crm_bp.route("/staff/<int:pu_id>/toggle_status", methods=["POST"])
@login_required
def staff_toggle_status(pu_id):
    """Owner suspends or reactivates a receptionist"""
    practice = CrmPractice.query.filter_by(owner_id=current_user.id).first()
    if not practice:
        flash("Unauthorized.", "error")
        return redirect(url_for('practice_crm_bp.dashboard'))
        
    pu = CrmPracticeUser.query.filter_by(id=pu_id, practice_id=practice.id).first()
    if not pu:
        flash("Receptionist not found.", "error")
        return redirect(url_for('practice_crm_bp.staff'))
        
    if pu.status == 'active':
        pu.status = 'suspended'
        flash("Receptionist suspended.", "success")
    else:
        pu.status = 'active'
        flash("Receptionist reactivated.", "success")
        
    db.session.commit()
    return redirect(url_for('practice_crm_bp.staff'))

@practice_crm_bp.route("/pipeline")
@login_required
def pipeline():
    """Receptionist kanban pipeline"""
    from app.models.auth import UserEnrollment, AuthSubject
    
    practice = None
    is_receptionist = False
    
    # Check enrollment status
    crm_subj = AuthSubject.query.filter_by(slug='practice_crm').first()
    if crm_subj:
        enr = UserEnrollment.query.filter_by(user_id=current_user.id, subject_id=crm_subj.id).first()
        if enr and enr.status == 'receptionist':
            is_receptionist = True

    if is_receptionist:
        pu = CrmPracticeUser.query.filter_by(user_id=current_user.id).first()
        if pu:
            if pu.status == 'suspended':
                flash("Your account has been suspended by the practice owner. Please contact them.", "error")
                return redirect(url_for('public_bp.welcome'))
            practice = CrmPractice.query.get(pu.practice_id)
        else:
            flash("Your Practice Owner has not added you to their staff list yet.", "warning")
            return redirect(url_for('public_bp.welcome'))
    else:
        practice = CrmPractice.query.filter_by(owner_id=current_user.id).first()
        
    if not practice:
        flash("You are not assigned to any practice.", "error")
        return redirect(url_for('public_bp.welcome'))
        
    enquiries = CrmEnquiry.query.filter_by(practice_id=practice.id).order_by(CrmEnquiry.created_at.desc()).all()
    return render_template("program_practice_crm/pipeline.html", practice=practice, enquiries=enquiries, is_receptionist=is_receptionist)


@practice_crm_bp.route("/appointments")
@login_required
def appointments_log():
    """Chronological log of all booked appointments"""
    from app.models.auth import UserEnrollment, AuthSubject
    
    practice = None
    is_receptionist = False
    
    # Check enrollment status
    crm_subj = AuthSubject.query.filter_by(slug='practice_crm').first()
    if crm_subj:
        enr = UserEnrollment.query.filter_by(user_id=current_user.id, subject_id=crm_subj.id).first()
        if enr and enr.status == 'receptionist':
            is_receptionist = True

    if is_receptionist:
        pu = CrmPracticeUser.query.filter_by(user_id=current_user.id).first()
        if pu:
            if pu.status == 'suspended':
                flash("Your account has been suspended by the practice owner. Please contact them.", "error")
                return redirect(url_for('public_bp.welcome'))
            practice = CrmPractice.query.get(pu.practice_id)
        else:
            flash("Your Practice Owner has not added you to their staff list yet.", "warning")
            return redirect(url_for('public_bp.welcome'))
    else:
        practice = CrmPractice.query.filter_by(owner_id=current_user.id).first()
        
    if not practice:
        flash("You are not assigned to any practice.", "error")
        return redirect(url_for('public_bp.welcome'))
        
    # Get all booked appointments
    now = datetime.utcnow()
    appointments = CrmEnquiry.query.filter_by(practice_id=practice.id, status='Booked')\
        .filter(CrmEnquiry.appointment_time != None)\
        .order_by(CrmEnquiry.appointment_time.asc()).all()
        
    upcoming = [a for a in appointments if a.appointment_time > now]
    past = [a for a in appointments if a.appointment_time <= now]
    # Reverse past so most recent past is at the top
    past.reverse()
    
    return render_template("program_practice_crm/appointments.html", 
                           practice=practice, 
                           upcoming=upcoming, 
                           past=past, 
                           is_receptionist=is_receptionist)



@practice_crm_bp.route("/enquiry/new", methods=["POST"])
@login_required
def new_enquiry():
    practice_id = request.form.get('practice_id')
    patient_name = request.form.get('patient_name')
    patient_id_no = request.form.get('patient_id_no')
    phone = request.form.get('phone')
    medical_aid = request.form.get("medical_aid", "").strip()
    medical_aid_plan = request.form.get("medical_aid_plan", "").strip()
    medical_aid_no = request.form.get("medical_aid_no", "").strip()
    reason = request.form.get("reason", "").strip()
    
    enquiry = CrmEnquiry(
        practice_id=practice_id,
        patient_name=patient_name,
        patient_id_no=patient_id_no,
        phone=phone,
        medical_aid=medical_aid,
        medical_aid_plan=medical_aid_plan,
        medical_aid_no=medical_aid_no,
        reason=reason,
        status='New',
        created_by_id=current_user.id
    )
    db.session.add(enquiry)
    db.session.flush()
    log_audit(enquiry.id, current_user.id, "Created enquiry")
    db.session.commit()
    flash("Enquiry logged successfully.", "success")
    return redirect(url_for('practice_crm_bp.pipeline'))

@practice_crm_bp.route("/enquiry/<int:id>/update", methods=["POST"])
@login_required
def update_enquiry(id):
    enquiry = CrmEnquiry.query.get_or_404(id)
    action_type = request.form.get('action_type')
    
    if action_type == 'request_verify':
        enquiry.status = 'Verification Pending'
        log_audit(enquiry.id, current_user.id, "Requested verification (Phone)")
        
    elif action_type == 'direct_verify':
        import random
        enquiry.verification_date = datetime.utcnow()
        enquiry.consultant_name = 'e-Switch (Simulated)'
        
        # Simulate API logic
        if not enquiry.medical_aid or 'Cash' in enquiry.medical_aid or 'Private' in enquiry.medical_aid:
            enquiry.funds_available = True
            enquiry.reference_no = 'N/A (Cash Patient)'
        else:
            # 80% chance of funds being available
            enquiry.funds_available = random.random() > 0.2
            enquiry.reference_no = f"AUTH-{random.randint(100000, 999999)}"
            
        if enquiry.funds_available:
            flash("Electronic verification successful. Funds available.", "success")
        else:
            flash("Electronic verification failed. Membership invalid or funds exhausted.", "warning")
            
        enquiry.status = 'Verified'
        log_audit(enquiry.id, current_user.id, f"Direct e-Verification (Funds: {'Yes' if enquiry.funds_available else 'No'})")
        
    elif action_type == 'record_verify':
        enquiry.verification_date = datetime.utcnow()
        enquiry.consultant_name = request.form.get('consultant_name')
        enquiry.funds_available = request.form.get('funds_available') == 'yes'
        enquiry.reference_no = request.form.get('reference_no')
        enquiry.status = 'Verified'
        log_audit(enquiry.id, current_user.id, f"Recorded verification (Funds: {'Yes' if enquiry.funds_available else 'No'})")
        
    elif action_type == 'offer_appt':
        enquiry.status = 'Appointment Offered'
        date_str = request.form.get('appointment_time')
        if date_str:
            try:
                enquiry.appointment_time = datetime.strptime(date_str, '%Y-%m-%dT%H:%M')
            except ValueError:
                pass
        log_audit(enquiry.id, current_user.id, "Offered appointment to patient")
        
    elif action_type == 'book':
        date_str = request.form.get('appointment_time')
        try:
            enquiry.appointment_time = datetime.strptime(date_str, '%Y-%m-%dT%H:%M')
            enquiry.status = 'Booked'
            log_audit(enquiry.id, current_user.id, f"Patient accepted, booked slot at {date_str}")
        except ValueError:
            flash("Invalid date format.", "error")
            return redirect(url_for('practice_crm_bp.pipeline'))
            
    elif action_type == 'not_booked':
        reason = request.form.get('not_booked_reason')
        if not reason or not reason.strip():
            flash("Reason required for not booking.", "error")
            return redirect(url_for('practice_crm_bp.pipeline'))
        enquiry.not_booked_reason = reason.strip()
        enquiry.status = 'Not Booked'
        log_audit(enquiry.id, current_user.id, f"Not booked: {enquiry.not_booked_reason}")
        
    db.session.commit()
    flash("Enquiry pipeline updated.", "success")
    return redirect(url_for('practice_crm_bp.pipeline'))

@practice_crm_bp.route("/enquiry/<int:id>/audit")
@login_required
def enquiry_audit(id):
    enquiry = CrmEnquiry.query.get_or_404(id)
    # Join with User to get the staff name
    audits = db.session.query(CrmAuditLog, User).join(User, CrmAuditLog.user_id == User.id)\
        .filter(CrmAuditLog.enquiry_id == id)\
        .order_by(CrmAuditLog.timestamp.asc()).all()
    return render_template("program_practice_crm/audit.html", enquiry=enquiry, audits=audits)
