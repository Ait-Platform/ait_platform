# app/public/routes.py
from flask import (
    Blueprint, request, session, redirect, url_for, 
    current_app, flash, render_template
)

from app.extensions import db
from sqlalchemy import select, text

try:
    from app.security import verify_provider_signature
except Exception:
    def verify_provider_signature(**kwargs):
        return None  # DEV: do nothing

from flask_mail import Message
from app.extensions import mail

public_bp = Blueprint("public_bp", __name__, template_folder="../../templates")

BRIDGE_EP = "auth_bp.bridge_dashboard"

@public_bp.route("/fix-sace")
def fix_sace():
    from app.models.auth import AuthSubject
    
    # 1. Deactivate old ones
    old_subjects = AuthSubject.query.filter(
        AuthSubject.slug.in_(['sace', 'cptd']) |
        AuthSubject.name.ilike('%cptd%') |
        AuthSubject.name.ilike('%sace%')
    ).all()
    
    for s in old_subjects:
        if s.slug == 'sace_hub':
            continue
        s.is_active = 0
        if s.slug in ['sace', 'cptd']:
            s.slug = f"{s.slug}_old_{s.id}"
        s.show_on_welcome = False
        s.is_hidden_on_bridge = True
            
    db.session.commit()
    
    # 2. Create or update the new SACE Hub
    new_sace = AuthSubject.query.filter_by(slug='sace_hub').first()
    if not new_sace:
        new_sace = AuthSubject(
            slug="sace_hub",
            name="SACE Activity Approval Hub",
            is_active=1,
            sort_order=100,
            commercial_mode="free",
            program_type="free",
            enroll_policy="auto_enroll",
            processor_default="yoco",
            show_on_welcome=True,
            about_endpoint="auth_bp.login",
            bypass_dashboard_endpoint="sace_bp.dashboard",
            start_endpoint="sace_bp.dashboard",
            is_hidden_on_bridge=False
        )
        db.session.add(new_sace)
    else:
        new_sace.name = "SACE Activity Approval Hub"
        new_sace.is_active = 1
        new_sace.show_on_welcome = True
        new_sace.is_hidden_on_bridge = False
        new_sace.about_endpoint = "auth_bp.login"
        new_sace.bypass_dashboard_endpoint = "sace_bp.dashboard"
        new_sace.start_endpoint = "sace_bp.dashboard"
        new_sace.commercial_mode = "free"
        new_sace.program_type = "free"
        new_sace.enroll_policy = "auto_enroll"
        new_sace.requires_price = 0
        new_sace.processor_default = "yoco"
        
    db.session.commit()
    
    # 3. Migrate enrollments from old 'sace' to 'sace_hub'
    from app.models.auth import UserEnrollment
    for old_s in old_subjects:
        if 'old' in old_s.slug:
            enrollments = UserEnrollment.query.filter_by(subject_id=old_s.id).all()
            for e in enrollments:
                existing = UserEnrollment.query.filter_by(user_id=e.user_id, subject_id=new_sace.id).first()
                if not existing:
                    e.subject_id = new_sace.id
    db.session.commit()
    
    return "Database updated successfully! Old tiles removed, new SACE Hub created, and enrollments migrated. Please go back to the Welcome page."

@public_bp.route("/privacy-policy")
def privacy_policy():
    return render_template("public/privacy_policy.html")

@public_bp.route("/terms")
def terms():
    return render_template("public/terms.html")

@public_bp.route("/refund-policy")
def refund_policy():
    return render_template("public/refund_policy.html")

@public_bp.route("/pricing")
def pricing():
    return render_template("public/pricing.html")

@public_bp.route("/contact", methods=["GET", "POST"])
def contact():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        email = request.form.get("email", "").strip()
        subject = request.form.get("subject", "").strip() or "Contact form"
        message = request.form.get("message", "").strip()

        to_addr = current_app.config.get("CONTACT_TO_EMAIL") or current_app.config.get("MAIL_USERNAME")

        # guardrail: ensure SMTP creds exist and won’t be None
        mu = current_app.config.get("MAIL_USERNAME")
        mp = current_app.config.get("MAIL_PASSWORD")
        if not mu or not mp:
            current_app.logger.error("Contact mail aborted: MAIL_USERNAME or MAIL_PASSWORD is missing/empty.")
            flash("Email temporarily unavailable. Please try again later.", "error")
            return redirect(url_for("public_bp.contact"))

        msg = Message(subject=f"[AIT Contact] {subject}", recipients=[to_addr])
        # Let Gmail auth sender be the default; set reply-to to the user
        msg.reply_to = email or None
        msg.body = (
            f"From: {name} <{email}>\n"
            f"Subject: {subject}\n\n"
            f"{message}\n"
        )
        mail.send(msg)
        flash("Thanks! Your message has been sent.", "success")
        return redirect(url_for("public_bp.contact"))

    return render_template("public/contact.html")

@public_bp.route("/_debug/routes")
def _debug_routes():
    from flask import current_app
    lines = [f"{r.endpoint:35s} -> {r.rule}" for r in current_app.url_map.iter_rules()]
    return "<pre>" + "\n".join(sorted(lines)) + "</pre>"

@public_bp.get("/")
def welcome():
    from sqlalchemy import text
    from app.extensions import db
    settings = {}
    try:
        rows = db.session.execute(text("SELECT key, value FROM system_settings")).fetchall()
    except Exception:
        db.session.rollback()
        # Fallback to create table if missing (solves Render deploy without migrations)
        try:
            db.session.execute(text('''
                CREATE TABLE IF NOT EXISTS system_settings (
                    key VARCHAR(255) PRIMARY KEY,
                    value VARCHAR(255) NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            '''))
            db.session.execute(text("INSERT INTO system_settings (key, value) VALUES ('mechanic_quote_cents', '1000') ON CONFLICT DO NOTHING"))
            db.session.execute(text("INSERT INTO system_settings (key, value) VALUES ('mechanic_invoice_cents', '1000') ON CONFLICT DO NOTHING"))
            db.session.commit()
            rows = db.session.execute(text("SELECT key, value FROM system_settings")).fetchall()
        except Exception:
            db.session.rollback()
            rows = []

    from werkzeug.routing import BuildError

    for row in rows:
        settings[row[0]] = row[1]

    from app.models.auth import AuthSubject

    # Auto-fix healthcore
    healthcore = AuthSubject.query.filter_by(slug='healthcore').first()
    if healthcore and (healthcore.name != 'Health IQ' or healthcore.about_endpoint != 'healthcore_bp.healthcore_about'):
        healthcore.name = 'Health IQ'
        healthcore.about_endpoint = 'healthcore_bp.healthcore_about'
        db.session.commit()
        
    # Auto-create staff subject if missing
    staff_subj = AuthSubject.query.filter_by(slug='staff').first()
    if not staff_subj:
        staff_subj = AuthSubject(
            slug='staff',
            name='Support Staff',
            program_type='system',
            commercial_mode='free',
            is_active=1,
            show_on_welcome=0,
            is_hidden_on_bridge=1,
            bypass_dashboard_endpoint='auth_bp.bridge_dashboard'
        )
        db.session.add(staff_subj)
        db.session.commit()
    elif not getattr(staff_subj, 'is_hidden_on_bridge', False):
        staff_subj.is_hidden_on_bridge = True
        db.session.commit()

    # Auto-fix legacy yoco database entries
    yoco_subjects = AuthSubject.query.filter_by(pay_endpoint='paystack_bp.paystack_start').all()
    if yoco_subjects:
        for subj in yoco_subjects:
            subj.pay_endpoint = 'paystack_bp.paystack_start'
        db.session.commit()

    subjects = (
        AuthSubject.query
        .filter(AuthSubject.is_active == 1)
        .filter(AuthSubject.show_on_welcome == True)
        .order_by(AuthSubject.name)
        .all()
    )

    for subj in subjects:
        subj.about_url = None

        # Don't show admin modules on the public page
        if subj.slug in ("admin", "admin_general"):
            continue

        if subj.about_endpoint:
            try:
                subj.about_url = url_for(subj.about_endpoint)
            except BuildError:
                subj.about_url = None

    #print(settings)

    return render_template(
        "public/welcome.html",
        settings=settings,
        subjects=subjects,
        staff_subj=staff_subj
    )

def refresh_bridge_session(user):
    """
    Mirrors your login() session-building so tiles on /dashboard are correct.
    """
    session["is_authenticated"] = True
    session["email"] = user.email

    is_admin_global = db.session.execute(
        text("SELECT 1 FROM auth_approved_admin WHERE lower(email)=lower(:e) LIMIT 1"),
        {"e": user.email},
    ).fetchone() is not None
    session["is_admin"] = bool(is_admin_global)

    admin_subject_rows = db.session.execute(text("""
        SELECT s.slug
        FROM auth_subject_admin sa
        JOIN auth_subject s ON s.id = sa.subject_id
        WHERE lower(sa.email) = lower(:e)
    """), {"e": user.email}).fetchall()
    session["admin_subjects"] = [r.slug for r in admin_subject_rows]

    # ✅ use user_enrollment (not auth_enrollment)
    enrolled_rows = db.session.execute(text("""
        SELECT s.slug
        FROM user_enrollment ue
        JOIN auth_subject s ON s.id = ue.subject_id
        WHERE ue.user_id = :uid AND ue.status = 'active'
    """), {"uid": user.id}).fetchall()
    session["enrolled_subjects"] = [r.slug for r in enrolled_rows]

    # ✅ use user_enrollment in the access check
    access_rows = db.session.execute(text("""
        SELECT
          s.slug,
          CASE
            WHEN :is_admin_global = 1 THEN 'admin'
            WHEN EXISTS (
              SELECT 1 FROM auth_subject_admin sa
              WHERE sa.subject_id = s.id AND lower(sa.email) = lower(:e)
            ) THEN 'admin'
            WHEN EXISTS (
              SELECT 1
              FROM user_enrollment ue
              WHERE ue.subject_id = s.id
                AND ue.user_id    = :uid
                AND ue.status     = 'active'
            ) THEN 'enrolled'
            ELSE 'locked'
          END AS access_level
        FROM auth_subject s
        WHERE s.is_active = 1
        ORDER BY s.sort_order, s.name
    """), {"e": user.email, "uid": user.id, "is_admin_global": 1 if is_admin_global else 0}).fetchall()
    session["subjects_access"] = {r.slug: r.access_level for r in access_rows}

@public_bp.route("/tutor/register", methods=["GET", "POST"])
def tutor_register():
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        name = request.form.get("name", "").strip()
        password = request.form.get("password", "")

        if not email or not password or not name:
            flash("All fields are required.", "error")
            return redirect(url_for("public_bp.tutor_register"))

        from app.models.auth import User, UserEnrollment, AuthSubject
        from flask_login import login_user

        user = User.query.filter(db.func.lower(User.email) == email).first()
        if not user:
            user = User(
                email=email,
                name=name,
                is_active=1
            )
            user.set_password(password)
            db.session.add(user)
            db.session.commit()

        # For now, only enroll as teacher in the 'home' subject 
        # since it's the only one with a Teacher Dashboard.
        home_subj = AuthSubject.query.filter_by(slug='home').first()
        if home_subj:
            enr = UserEnrollment.query.filter_by(user_id=user.id, subject_id=home_subj.id).first()
            if not enr:
                enr = UserEnrollment(
                    user_id=user.id,
                    subject_id=home_subj.id,
                    status='teacher'
                )
                db.session.add(enr)
            else:
                enr.status = 'teacher'
        
        db.session.commit()
        
        login_user(user)
        flash("Tutor registration successful! You are now available for learners to select.", "success")
        return redirect(url_for("auth_bp.bridge_dashboard"))

    return render_template("public/tutor_register.html")

@public_bp.route("/receptionist/register", methods=["GET", "POST"])
def receptionist_register():
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        name = request.form.get("name", "").strip()
        password = request.form.get("password", "")

        if not email or not password or not name:
            flash("All fields are required.", "error")
            return redirect(url_for("public_bp.receptionist_register"))

        from app.models.auth import User, UserEnrollment, AuthSubject

        user = User.query.filter(db.func.lower(User.email) == email).first()
        if not user:
            user = User(
                email=email,
                name=name,
                is_active=1
            )
            user.set_password(password)
            db.session.add(user)
            db.session.commit()

        # Enroll as receptionist for practice_crm
        crm_subj = AuthSubject.query.filter_by(slug='practice_crm').first()
        if crm_subj:
            enr = UserEnrollment.query.filter_by(user_id=user.id, subject_id=crm_subj.id).first()
            if not enr:
                enr = UserEnrollment(
                    user_id=user.id,
                    subject_id=crm_subj.id,
                    status='receptionist'
                )
                db.session.add(enr)
            else:
                enr.status = 'receptionist'
        
        db.session.commit()
        
        flash(f"Registration successful! Your practice owner can now add your email ({email}) to their staff list.", "success")
        return redirect(url_for("public_bp.welcome"))

    return render_template("public/receptionist_register.html")

@public_bp.route("/coming-soon/<subject_slug>")
def coming_soon(subject_slug):
    return render_template("public/coming_soon.html", subject_slug=subject_slug)



@public_bp.route("/programs")
def programs_list():
    from app.models.auth import AuthSubject
    # Query all active subjects for the Programs Directory that are meant to be shown
    subjects = AuthSubject.query.filter_by(is_active=1, show_on_welcome=True).order_by(AuthSubject.name).all()
    return render_template("public/programs.html", subjects=subjects)

