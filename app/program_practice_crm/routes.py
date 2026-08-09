
from flask import render_template, request, redirect, url_for, flash, abort, session
from flask_login import current_user, login_required
from sqlalchemy import text
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
    """Welcome / Sales page for Medical Practice Customer Relation Management"""
    return render_template("program_practice_crm/about.html")

@practice_crm_bp.route("/communication-logs")
@login_required
def communication_logs():
    from app.models.auth import InviteLog
    logs = InviteLog.query.filter_by(sender_id=current_user.id, program_slug="practice_crm").order_by(InviteLog.sent_at.desc()).all()
    return render_template("shared/invite_logs_page.html", logs=logs, is_admin_view=False, back_url=url_for("practice_crm_bp.pipeline"))


@practice_crm_bp.route("/price")
def price_page():
    from app.models.auth import AuthSubject
    from app.enrollment.logic import get_quote_for_subject_country
    
    subject = AuthSubject.query.filter(db.func.lower(AuthSubject.slug) == 'practice_crm').first()
    if not subject:
        flash("Subject not found.", "warning")
        return redirect(url_for('public_bp.welcome'))

    country_code = (request.args.get("country") or "").strip().upper()
    if not country_code and current_user.is_authenticated:
        ent = db.session.execute(text("""
            SELECT ue.country_code 
              FROM user_enrollment ue
              JOIN auth_subject s ON s.id = ue.subject_id
             WHERE ue.user_id = :uid AND s.slug = 'practice_crm'
        """), {"uid": current_user.id}).mappings().first()
        if ent and ent["country_code"]:
            country_code = ent["country_code"]

    if not country_code:
        country_code = session.get("country_code", "")

    if country_code:
        session["country_code"] = country_code

    price_ctx = {
        "has_quote": False,
        "price_id": None,
        "country_code": None,
        "local_amount": None,
        "local_currency": None,
        "estimated_zar": None,
        "fx_rate": None,
        "is_discount": False,
    }

    if country_code:
        row = get_quote_for_subject_country(subject.id, country_code)
        if row:
            price_ctx.update({
                "price_id": row.id,
                "country_code": row.country_code,
                "local_amount": row.local_amount_cents,
                "local_currency": row.local_currency,
                "estimated_zar": row.zar_amount_cents,
                "fx_rate": getattr(row, "fx_rate", None),
                "is_discount": getattr(row, "is_discount", False),
            })
            price_ctx["has_quote"] = True
        else:
            flash("No pricing found for that country yet.", "warning")

    from app.utils.country import get_active_countries
    countries = get_active_countries()

    from app.models.billing import TokenTariff
    tariff = TokenTariff.query.filter_by(program_slug='practice_crm', action_name='enquiry_intake').first()
    enquiry_cents = tariff.base_token_cost * 100 if tariff else 1000

    return render_template("program_practice_crm/price.html", price=price_ctx, subject=subject, countries=countries, enquiry_cents=enquiry_cents)

@practice_crm_bp.route("/migrate_db")
def migrate_db():
    from sqlalchemy import text
    from app.models.auth import UserEnrollment, AuthSubject
    try:
        db.create_all()
        
        # Migrate practice CRM token cost to new universal table (convert from cents to tokens if needed)
        from app.models.billing import TokenTariff
        val = db.session.execute(text("SELECT value FROM system_settings WHERE key = 'practice_enquiry_cents'")).scalar()
        
        # If val is 1000 (cents), we convert it to 10 (tokens). If it's already small, assume tokens.
        parsed_val = int(float(val)) if val else 1000
        enquiry_cost = 10 if parsed_val == 1000 else (parsed_val // 100 if parsed_val >= 100 else parsed_val)
        
        if not TokenTariff.query.filter_by(program_slug='practice_crm', action_name='enquiry_intake').first():
            db.session.add(TokenTariff(program_slug='practice_crm', action_name='enquiry_intake', base_token_cost=enquiry_cost))
            
        # Fix any existing 1000 token costs to 10
        db.session.execute(text("UPDATE universal_token_tariff SET base_token_cost = 10 WHERE program_slug = 'practice_crm' AND action_name = 'enquiry_intake' AND base_token_cost >= 100"))
        
        # Seed Signup Bonus for HP CRM
        from app.models.billing import SignupBonus
        if not SignupBonus.query.filter_by(program_slug='practice_crm').first():
            db.session.add(SignupBonus(program_slug='practice_crm', bonus_tokens=100))
            
        # Migrate existing HP CRM practices to use AitTokenWallet
        from app.models.auth import AitTokenWallet, AitTokenTransaction
        db.session.execute(text("""
            INSERT INTO ait_token_wallet (user_id, balance)
            SELECT owner_id, COALESCE(wallet_balance_cents / 100, 0)
            FROM crm_practice
            WHERE owner_id NOT IN (SELECT user_id FROM ait_token_wallet)
        """))
        
        # We also need to seed the new test accounts with the 100 token bonus if they don't have it
        db.session.execute(text("""
            UPDATE ait_token_wallet 
            SET balance = 100 
            WHERE balance = 0 AND user_id IN (SELECT owner_id FROM crm_practice)
        """))
        
            
        # Migrate Cultural Fire token costs to new universal table
        try:
            cfi_tariffs = db.session.execute(text("SELECT action_name, base_token_cost FROM cfi_token_tariff")).fetchall()
            for row in cfi_tariffs:
                if not TokenTariff.query.filter_by(program_slug='culturalfire', action_name=row[0]).first():
                    db.session.add(TokenTariff(program_slug='culturalfire', action_name=row[0], base_token_cost=row[1]))
        except:
            pass # cfi_token_tariff might not exist or already dropped
            
        db.session.commit()
        
        # Add patient_id to crm_enquiry
        try:
            db.session.execute(text("ALTER TABLE crm_enquiry ADD COLUMN patient_id INTEGER REFERENCES crm_patient(id);"))
            db.session.commit()
        except Exception:
            db.session.rollback()
            
        # Add billing property metro columns
        try:
            db.session.execute(text("ALTER TABLE bil_property ADD COLUMN metro_arrangement_amount FLOAT DEFAULT 0.0;"))
            db.session.execute(text("ALTER TABLE bil_property ADD COLUMN metro_arrangement_duration INTEGER DEFAULT 0;"))
            db.session.execute(text("ALTER TABLE bil_property ADD COLUMN metro_rates_amount FLOAT DEFAULT 0.0;"))
            db.session.execute(text("ALTER TABLE bil_property ADD COLUMN metro_valuation FLOAT DEFAULT 0.0;"))
            db.session.commit()
        except Exception:
            db.session.rollback()
            
        # Fix the Support Staff enrollments issue
        staff_subj = AuthSubject.query.filter_by(slug='staff').first()
        crm_subj = AuthSubject.query.filter_by(slug='practice_crm').first()
        if staff_subj and crm_subj:
            enrs = UserEnrollment.query.filter_by(subject_id=staff_subj.id).all()
            for e in enrs:
                # Check if they are already enrolled in practice_crm to avoid duplicates
                existing = UserEnrollment.query.filter_by(user_id=e.user_id, subject_id=crm_subj.id).first()
                if not existing:
                    e.subject_id = crm_subj.id
                else:
                    db.session.delete(e) # Delete duplicate staff enrollment
            
            # Hide staff tile
            staff_subj.is_hidden_on_bridge = True
            db.session.commit()
            
        return "Migration successful! Please return to the dashboard."
    except Exception as e:
        db.session.rollback()
        return f"Error: {str(e)}"



@practice_crm_bp.route("/patients")
@login_required
def patients_directory():
    """Patient directory view"""
    from app.models.practice_crm import CrmPatient
    
    practice = CrmPractice.query.filter_by(owner_id=current_user.id).first()
    if not practice:
        # Check if they are staff
        pu = CrmPracticeUser.query.filter_by(user_id=current_user.id, status='active').first()
        if pu:
            practice = CrmPractice.query.get(pu.practice_id)
            
    if not practice:
        flash("You do not have access to a practice CRM.", "error")
        return redirect(url_for('public_bp.welcome'))
        
    patients = CrmPatient.query.filter_by(practice_id=practice.id).order_by(CrmPatient.created_at.desc()).all()
    return render_template("program_practice_crm/patients.html", practice=practice, patients=patients)

@practice_crm_bp.route("/settings/setup", methods=["GET", "POST"])
@login_required
def practice_settings_setup():
    """Owner dashboard: update practice biodata"""
    practice = CrmPractice.query.filter_by(owner_id=current_user.id).first()
    if not practice:
        flash("Please visit the dashboard first to initialize your practice.", "warning")
        return redirect(url_for('practice_crm_bp.pipeline'))
        
    if request.method == "POST":
        practice.name = request.form.get("name", "").strip()
        practice.email = request.form.get("email", "").strip()
        practice.phone = request.form.get("phone", "").strip()
        practice.address = request.form.get("address", "").strip()
        practice.dentist_details = request.form.get("dentist_details", "").strip()
        practice.operating_hours = request.form.get("operating_hours", "").strip()
        practice.slot_settings = request.form.get("slot_settings", "").strip()
        
        db.session.commit()
        flash("Practice setup updated successfully.", "success")
        return redirect(url_for("practice_crm_bp.practice_settings_setup"))
        
    return render_template("program_practice_crm/settings_setup.html", practice=practice, active_tab="setup")

@practice_crm_bp.route("/settings/wallet", methods=["GET"])
@login_required
def practice_settings_wallet():
    """Owner dashboard: Token Wallet"""
    practice = CrmPractice.query.filter_by(owner_id=current_user.id).first()
    if not practice:
        flash("Please visit the dashboard first to initialize your practice.", "warning")
        return redirect(url_for('practice_crm_bp.pipeline'))
        
    from app.models.auth import AitTokenWallet
    wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
    token_balance = wallet.balance if wallet else 0
        
    return render_template("program_practice_crm/settings_wallet.html", practice=practice, token_balance=token_balance, active_tab="wallet")

@practice_crm_bp.route("/settings/api-keys", methods=["GET", "POST"])
@login_required
def practice_settings_api_keys():
    """Owner dashboard: API Keys & Integrations"""
    practice = CrmPractice.query.filter_by(owner_id=current_user.id).first()
    if not practice:
        flash("Please visit the dashboard first to initialize your practice.", "warning")
        return redirect(url_for('practice_crm_bp.pipeline'))
        
    if request.method == "POST":
        practice.clearing_house_provider = request.form.get("clearing_house_provider", "").strip()
        practice.clearing_house_api_key = request.form.get("clearing_house_api_key", "").strip()
        
        db.session.commit()
        flash("API Key settings updated successfully.", "success")
        return redirect(url_for("practice_crm_bp.practice_settings_api_keys"))
        
    return render_template("program_practice_crm/settings_api.html", practice=practice, active_tab="api_keys")

@practice_crm_bp.route("/settings", methods=["GET"])
@login_required
def practice_settings():
    return redirect(url_for("practice_crm_bp.practice_settings_setup"))

@practice_crm_bp.route("/staff", methods=["GET", "POST"])
@login_required
def staff():
    """Manage receptionists"""
    practice = CrmPractice.query.filter_by(owner_id=current_user.id).first()
    if not practice:
        flash("Please visit the dashboard first to initialize your practice.", "warning")
        return redirect(url_for('practice_crm_bp.pipeline'))
        
    if request.method == "POST":
        email = request.form.get('email', '').strip().lower()
        name = request.form.get('name', '').strip()
        password = request.form.get('password', '')
        
        user = User.query.filter_by(email=email).first()
        
        if not user:
            if not password:
                flash(f"A password is required to create a new account for {email}.", "error")
                return redirect(url_for('practice_crm_bp.staff'))
            
            user = User(email=email, name=name, is_active=1)
            user.set_password(password)
            db.session.add(user)
            db.session.flush() # flush to get user.id
            flash(f"Successfully created a new account for {name}.", "success")
            
        else:
            # If user exists but name was provided, maybe update it? Or just leave it.
            pass

        # Check if already in practice
        existing = CrmPracticeUser.query.filter_by(practice_id=practice.id, user_id=user.id).first()
        if existing:
            flash(f"{user.name or email} is already a receptionist.", "info")
        else:
            pu = CrmPracticeUser(practice_id=practice.id, user_id=user.id, role='receptionist')
            db.session.add(pu)
            db.session.commit()
            flash(f"Added {user.name or email} to your CRM pipeline.", "success")
            
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
        return redirect(url_for('practice_crm_bp.pipeline'))
        
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
        return redirect(url_for('practice_crm_bp.pipeline'))
        
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

@practice_crm_bp.route("/setup", methods=["GET", "POST"])
@login_required
def setup():
    """First-time setup for practice owner"""
    practice = CrmPractice.query.filter_by(owner_id=current_user.id).first()
    
    if request.method == "POST":
        if not practice:
            from app.models.billing import SignupBonus
            from app.models.auth import AitTokenWallet, AitTokenTransaction
            bonus = SignupBonus.query.filter_by(program_slug='practice_crm').first()
            start_tokens = bonus.bonus_tokens if bonus else 0
            
            practice = CrmPractice(owner_id=current_user.id)
            
            # Check if owner has a wallet
            wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
            if not wallet:
                wallet = AitTokenWallet(user_id=current_user.id, balance=start_tokens)
                db.session.add(wallet)
                if start_tokens > 0:
                    db.session.flush()
                    txn = AitTokenTransaction(wallet_id=wallet.id, amount=start_tokens, transaction_type="purchase", description="HP CRM Signup Bonus")
                    db.session.add(txn)
            db.session.add(practice)
            
        practice.name = request.form.get("name")
        practice.practice_type = request.form.get("practice_type")
        practice.phone = request.form.get("phone")
        practice.email = request.form.get("email")
        
        db.session.commit()
        flash("Practice setup successfully!", "success")
        return redirect(url_for('practice_crm_bp.pipeline'))
        
    return render_template("program_practice_crm/setup.html", practice=practice)

PRACTICE_TREATMENTS = {
    "Dentist": ["General Checkup & Cleaning", "Toothache / Pain", "Cavity / Filling", "Root Canal", "Extraction", "Teeth Whitening", "Crown / Bridge", "Orthodontic Consult"],
    "General Practitioner": ["General Checkup", "Prescription Renewal", "Fever / Flu Symptoms", "Blood Tests", "Referral Letter", "Medical Certificate", "Chronic Care"],
    "Optometrist": ["Eye Test", "Glasses Fitment", "Contact Lenses", "Glaucoma Check", "Dry Eyes"],
    "Physiotherapist": ["Back Pain", "Neck Pain", "Sports Injury", "Post-op Rehab", "Massage Therapy", "Dry Needling"],
    "Psychologist": ["Initial Consultation", "Follow-up Therapy", "Couples Counseling", "Trauma Counseling", "Child Therapy"],
    "Dietician": ["Weight Management", "Meal Planning", "Diabetes Diet Plan", "Sports Nutrition", "Cholesterol Management"],
    "Chiropractor": ["Back Adjustment", "Neck Adjustment", "Joint Pain", "Sciatica", "Posture Correction"],
    "Specialist": ["Initial Consultation", "Follow-up Consultation", "Pre-op Assessment", "Post-op Checkup", "Second Opinion"],
    "Other": ["General Consultation", "Follow-up", "Specific Treatment"]
}

@practice_crm_bp.route("/pipeline")
@login_required
def pipeline():
    try:
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
                # Create default practice if none exists for a new owner
                from app.models.billing import SignupBonus
                from app.models.auth import AitTokenWallet, AitTokenTransaction
                bonus = SignupBonus.query.filter_by(program_slug='practice_crm').first()
                start_tokens = bonus.bonus_tokens if bonus else 0
                
                practice = CrmPractice(owner_id=current_user.id, name=f"{current_user.name or 'My'} Practice")
                
                # Check if owner has a wallet
                wallet = AitTokenWallet.query.filter_by(user_id=current_user.id).first()
                if not wallet:
                    wallet = AitTokenWallet(user_id=current_user.id, balance=start_tokens)
                    db.session.add(wallet)
                    if start_tokens > 0:
                        db.session.flush()
                        txn = AitTokenTransaction(wallet_id=wallet.id, amount=start_tokens, transaction_type="purchase", description="HP CRM Signup Bonus")
                        db.session.add(txn)
                db.session.add(practice)
                db.session.commit()
                
        if not practice:
            flash("You are not assigned to any practice.", "error")
            return redirect(url_for('public_bp.welcome'))
            
        if not practice.practice_type and not is_receptionist:
            return redirect(url_for('practice_crm_bp.setup'))
            
        treatments = PRACTICE_TREATMENTS.get(practice.practice_type, PRACTICE_TREATMENTS["Other"])
            
        from app.models.auth import AitTokenWallet
        wallet = AitTokenWallet.query.filter_by(user_id=practice.owner_id).first()
        token_balance = wallet.balance if wallet else 0
        
        enquiries = CrmEnquiry.query.filter_by(practice_id=practice.id).order_by(CrmEnquiry.created_at.desc()).all()
        return render_template("program_practice_crm/pipeline.html", practice=practice, enquiries=enquiries, is_receptionist=is_receptionist, treatments=treatments, token_balance=token_balance)
    except Exception as e:
        import traceback
        return f"<pre>Error occurred:\n{traceback.format_exc()}</pre>", 500


@practice_crm_bp.route("/appointments")
@login_required
def appointments_log():
    try:
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
    except Exception as e:
        import traceback
        return f"<pre>Error in appointments_log:\n{traceback.format_exc()}</pre>", 500



@practice_crm_bp.route("/api/patients/search")
@login_required
def search_patients():
    from flask import jsonify
    practice_id = request.args.get('practice_id')
    query = request.args.get('q', '').strip()
    
    if not practice_id or not query:
        return jsonify([])
        
    # Ensure current user has access to this practice
    has_access = False
    pu = CrmPracticeUser.query.filter_by(user_id=current_user.id, practice_id=practice_id).first()
    if pu and pu.status == 'active':
        has_access = True
    else:
        practice = CrmPractice.query.filter_by(id=practice_id, owner_id=current_user.id).first()
        if practice:
            has_access = True
            
    if not has_access:
        return jsonify([])
        
    enquiries = CrmEnquiry.query.filter_by(practice_id=practice_id)\
        .filter((CrmEnquiry.patient_name.ilike(f'%{query}%')) | (CrmEnquiry.patient_id_no.ilike(f'%{query}%')))\
        .order_by(CrmEnquiry.created_at.desc())\
        .limit(20).all()
        
    results = []
    seen = set()
    for e in enquiries:
        key = e.patient_name.lower().strip()
        if key not in seen:
            seen.add(key)
            results.append({
                'name': e.patient_name,
                'id_no': e.patient_id_no or '',
                'phone': e.phone or '',
                'medical_aid': e.medical_aid or '',
                'medical_aid_plan': e.medical_aid_plan or '',
                'medical_aid_no': e.medical_aid_no or ''
            })
            
    return jsonify(results)

@practice_crm_bp.route("/enquiry/new", methods=["POST"])
@login_required
def new_enquiry():
    practice_id = request.form.get('practice_id')
    
    # Billing logic
    practice = CrmPractice.query.get(practice_id)
    if not practice:
        flash("Practice not found.", "error")
        return redirect(url_for('practice_crm_bp.pipeline'))
        
    from app.models.billing import TokenTariff
    from app.models.auth import AitTokenWallet, AitTokenTransaction
    
    tariff = TokenTariff.query.filter_by(program_slug='practice_crm', action_name='enquiry_intake').first()
    enquiry_cost_tokens = tariff.base_token_cost if tariff else 10
    
    # Get practice owner's wallet
    wallet = AitTokenWallet.query.filter_by(user_id=practice.owner_id).first()
    token_balance = wallet.balance if wallet else 0
    
    if token_balance < enquiry_cost_tokens:
        flash("Insufficient tokens. Please top up your wallet to continue.", "warning")
        return redirect(url_for('practice_crm_bp.mock_bill'))
    
    # Deduct tokens
    wallet.balance -= enquiry_cost_tokens
    txn = AitTokenTransaction(wallet_id=wallet.id, amount=-enquiry_cost_tokens, transaction_type="purchase", description="HP CRM Enquiry Intake")
    db.session.add(txn)

    patient_name = request.form.get('patient_name')
    patient_id_no = request.form.get('patient_id_no')
    phone = request.form.get('phone', '').strip()
    
    if not phone:
        flash("A phone number is essential, even for walk-in patients.", "error")
        return redirect(url_for('practice_crm_bp.pipeline'))

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
        practice = CrmPractice.query.get(enquiry.practice_id)
        has_byok = bool(practice.clearing_house_api_key)
            
        import random
        enquiry.verification_date = datetime.utcnow()
        enquiry.consultant_name = f"{practice.clearing_house_provider.title() if has_byok else 'AIT Master Switch'} (Simulated)"
        
        # Simulate API logic
        if not enquiry.medical_aid or 'Cash' in enquiry.medical_aid or 'Private' in enquiry.medical_aid:
            enquiry.funds_available = True
            enquiry.reference_no = 'N/A (Cash Patient)'
        else:
            # 80% chance of funds being available
            enquiry.funds_available = random.random() > 0.2
            enquiry.reference_no = f"AUTH-{random.randint(100000, 999999)}"
            
        if enquiry.funds_available:
            if has_byok:
                flash("Electronic verification successful using your Custom API Key.", "success")
            else:
                flash("Electronic verification successful (AIT Master Key).", "success")
        else:
            if has_byok:
                flash("Electronic verification failed. Membership invalid or funds exhausted (Custom API Key).", "warning")
            else:
                flash("Electronic verification failed. Membership invalid or funds exhausted (AIT Master Key).", "warning")
            
        enquiry.status = 'Verified'
        log_audit(enquiry.id, current_user.id, f"Direct e-Verification via {'BYOK' if has_byok else 'AIT Key'} (Funds: {'Yes' if enquiry.funds_available else 'No'})")
        
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
            
            # --- Create Patient Profile ---
            first_name = request.form.get('first_name', '').strip()
            last_name = request.form.get('last_name', '').strip()
            
            if not first_name and not last_name:
                # Derive from patient_name
                parts = enquiry.patient_name.strip().split(' ', 1)
                first_name = parts[0]
                if len(parts) > 1:
                    last_name = parts[1]
                    
            from app.models.practice_crm import CrmPatient
            patient = CrmPatient(
                practice_id=enquiry.practice_id,
                first_name=first_name,
                last_name=last_name,
                id_number=enquiry.patient_id_no,
                phone=enquiry.phone,
                email=request.form.get('email', '').strip() or None,
                address=request.form.get('address', '').strip() or None,
                medical_aid=enquiry.medical_aid,
                medical_aid_plan=enquiry.medical_aid_plan,
                medical_aid_no=enquiry.medical_aid_no
            )
            
            dob_str = request.form.get('dob')
            if dob_str:
                try:
                    patient.dob = datetime.strptime(dob_str, '%Y-%m-%d').date()
                except ValueError:
                    pass
                    
            db.session.add(patient)
            db.session.flush() # get patient.id
            enquiry.patient_id = patient.id
            
            log_audit(enquiry.id, current_user.id, f"Patient accepted, booked slot at {date_str} and Patient Profile Created")
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

@practice_crm_bp.route('/mock_bill')
@login_required
def mock_bill():
    practice = CrmPractice.query.filter_by(owner_id=current_user.id).first()
    if not practice:
        flash("You do not have an active practice.", "warning")
        return redirect(url_for('practice_crm_bp.pipeline'))
        
    from app.models.auth import AitTokenWallet
    wallet = AitTokenWallet.query.filter_by(user_id=practice.owner_id).first()
    token_balance = wallet.balance if wallet else 0
        
    return render_template("program_practice_crm/mock_bill.html", practice=practice, token_balance=token_balance)

@practice_crm_bp.route("/topup", methods=["GET", "POST"])
@login_required
def topup():
    """Route to redirect user to Paystack checkout for practice CRM tokens"""
    from flask import session
    # 100 ZAR = 10000 cents, for a bundle of 100 Tokens
    session["practice_crm_topup_amount_cents"] = 10000 
    session["topup_tokens"] = 100
    return redirect(url_for('paystack_bp.paystack_start', subject='practice_crm_topup', email=current_user.email))


