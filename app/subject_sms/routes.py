from flask import (
    Blueprint, abort, current_app, flash, jsonify, render_template, 
    redirect, send_from_directory, url_for, request, session
)
from sqlalchemy import text
from app.models.auth import AuthSubject, User
from app.extensions import db
from flask_login import login_required, current_user, login_user
from datetime import date
from app.models.sms import (
    SmsAccessLog, SmsApprovedUser, SmsFinCategory, SmsFinTxn, SmsGuardian, SmsLearnerGuardian, SmsRole, SmsRoleAssignment,
    SmsSchool, SmsSgbMember, SmsSgbMeeting, SmsLearner, SmsTeacher, SmsMgmtTask,
)
from datetime import datetime
from sqlalchemy import func
from app.subject_sms.helpers import (
    _current_sms_school,
    _require_sms_auditor_school,
    _sms_owner_school,
    has_sms_audit_access,
    has_sms_finance_access,
    has_sms_role,
)
from types import SimpleNamespace
from werkzeug.security import generate_password_hash

sms_bp = Blueprint(
    "sms_bp",
    __name__,
    template_folder="templates",
    url_prefix="/subject/sms"
)


SMS_MODULES = [
    {"title": "Governance",      "text": "School leadership, roles, and legal compliance."},
    {"title": "Learners",        "text": "Admissions, support, attendance, progression."},
    {"title": "Teachers",        "text": "Workload, development, appraisal."},
    {"title": "Management",      "text": "SMT operations and decision-making."},
    {"title": "Finance",         "text": "Fees, billing, budgeting and expenditure."},
    {"title": "Assets",          "text": "Inventory, textbooks, ICT and physical resources."},
    {"title": "Documentation",   "text": "Registers, policies, minutes, reports."},
    {"title": "Year Progression","text": "Promotions, captures, schedules."},
    {"title": "Compliance",      "text": "DBE, SASA, reporting and policies."},
    {"title": "Communication",   "text": "Parents, staff, notices and messaging."},
]



@sms_bp.get("/about")
def about():
    """About page 1 – first 4 tiles."""
    page_modules = SMS_MODULES[0:4]
    return render_template("subject/sms/about.html", modules=page_modules)

@sms_bp.get("/about/details", endpoint="about_details")
def about_details():
    """About page 2 – next 4 tiles."""
    page_modules = SMS_MODULES[4:8]
    return render_template("subject/sms/about_details.html", modules=page_modules)

@sms_bp.get("/about/communication", endpoint="about_comm")
def about_comm():
    """About page 3 – last 2 tiles."""
    page_modules = SMS_MODULES[8:10]
    return render_template("subject/sms/about_comm.html", modules=page_modules)

@sms_bp.route("/about/details")
def sms_about_details():
    price_cents = get_sms_price_cents()
    return render_template("subject/sms/about_details.html", price_cents=price_cents)

def get_sms_price_cents():
    # Find the SMS subject row (slug or id, whichever you are using)
    subj = AuthSubject.query.filter_by(slug="sms").first()
    if not subj:
        return None

    cents = db.session.execute(
        text("""
            SELECT amount_cents
              FROM auth_pricing
             WHERE subject_id = :sid
               AND role = 'user'
               AND plan = 'enrollment'
               AND COALESCE(is_active::int, 1) = 1
             ORDER BY active_from DESC, id DESC
             LIMIT 1
        """),
        {"sid": subj.id},
    ).scalar()

    return int(cents) if cents is not None else None

@sms_bp.get("/start", endpoint="start_sms")
def sms_start():
    """
    Entry point from the SMS About / Start Program button.
    Anonymous users should go to registration, not login.
    """
    # After registration, they should land on the learner dashboard for SMS
    next_url = request.args.get("next") or url_for("sms_bp.sms_entry")


    return redirect(
        url_for(
            "auth_bp.register",
            # we only have 'user' and 'admin'; SMS schools are 'user'
            role="user",
            subject="sms",
            next=next_url,
        )
    )


@sms_bp.get("/home")
@login_required
def subject_home():
    school = _current_sms_school()
    if not school:
        return redirect(url_for("sms_bp.setup_dashboard"))
    return render_template("subject/sms/home.html", school=school)


@sms_bp.route("/setup", methods=["GET", "POST"])
@login_required
def setup_school():
    """One simple form to capture the school profile."""
    school = _current_sms_school()

    if request.method == "POST":
        name     = (request.form.get("name") or "").strip()
        emis_no  = (request.form.get("emis_no") or "").strip()
        phase    = (request.form.get("phase") or "").strip()
        quintile = (request.form.get("quintile") or "").strip()
        learners = request.form.get("learners") or None

        if not name:
            flash("Please enter your school name.", "warning")
        else:
            if school is None:
                school = SmsSchool(user_id=current_user.id)
                db.session.add(school)

            school.name     = name
            school.emis_no  = emis_no or None
            school.phase    = phase or None
            school.quintile = quintile or None
            school.learners = int(learners) if learners else None

            db.session.commit()
            flash("School profile saved.", "success")
            return render_template("subject/sms/school_profile.html", school=school)



    return render_template("subject/sms/setup_school.html", school=school)

@sms_bp.get("/governance")
@login_required
def governance_home():
    school = _current_sms_school()
    if not school:
        flash("Please complete your school setup first.", "warning")
        sms_bp.setup_school

    # --- which year to show? ---
    # ?year=2025 in the URL; default = current year
    year = request.args.get("year", type=int)
    if not year:
        year = date.today().year

    # --- load all active members for the school ---
    all_members = (
        SmsSgbMember.query
        .filter_by(school_id=school.id, is_active=True)
        .order_by(SmsSgbMember.start_date.asc(), SmsSgbMember.full_name.asc())
        .all()
    )

    # available years (for a dropdown in the template)
    available_years = sorted(
        {
            (m.start_date or date.today()).year
            for m in all_members
        },
        reverse=True,
    )

    # members for the selected year only
    members = [
        m for m in all_members
        if (m.start_date or date.today()).year == year
    ]

    meetings = (
        SmsSgbMeeting.query
        .filter_by(school_id=school.id)
        .order_by(SmsSgbMeeting.meeting_date.desc())
        .limit(5)
        .all()
    )

    return render_template(
        "subject/sms/governance/index.html",
        school=school,
        members=members,
        meetings=meetings,
        year=year,
        available_years=available_years,
    )

@sms_bp.route("/governance/member/new", methods=["GET", "POST"])
@login_required
def governance_member_new():
    school = _current_sms_school()
    if not school:
        flash("Please complete your school setup first.", "warning")
        sms_bp.setup_school

    if request.method == "POST":
        full_name = (request.form.get("full_name") or "").strip()
        role      = (request.form.get("role")      or "").strip()
        start_str = (request.form.get("start_date") or "").strip()

        if not full_name or not role:
            flash("Name and role are required.", "danger")
            return redirect(url_for("sms_bp.governance_member_new"))

        start_date = None
        if start_str:
            try:
                parts = [int(p) for p in start_str.split("-")]  # YYYY-MM-DD
                start_date = date(parts[0], parts[1], parts[2])
            except Exception:
                flash("Start date must be YYYY-MM-DD.", "warning")

        m = SmsSgbMember(
            school_id=school.id,
            full_name=full_name,
            role=role,
            start_date=start_date,
            is_active=True,
        )
        db.session.add(m)
        db.session.commit()

        flash("SGB member added.", "success")
        return redirect(url_for("sms_bp.governance_home"))

    return render_template("subject/sms/governance/member_form.html", school=school)

@sms_bp.route("/governance/meeting/new", methods=["GET", "POST"])
@login_required
def governance_meeting_new():
    school = _current_sms_school()
    if not school:
        flash("Please complete your school setup first.", "warning")
        sms_bp.setup_school

    if request.method == "POST":
        date_str = (request.form.get("meeting_date") or "").strip()
        term_str = (request.form.get("term") or "").strip()
        agenda   = (request.form.get("agenda") or "").strip()
        minutes  = (request.form.get("minutes") or "").strip()

        if not date_str:
            flash("Meeting date is required.", "danger")
            return redirect(url_for("sms_bp.governance_meeting_new"))

        try:
            parts = [int(p) for p in date_str.split("-")]
            meeting_date = date(parts[0], parts[1], parts[2])
        except Exception:
            flash("Meeting date must be YYYY-MM-DD.", "warning")
            return redirect(url_for("sms_bp.governance_meeting_new"))

        term = int(term_str) if term_str.isdigit() else None

        mtg = SmsSgbMeeting(
            school_id=school.id,
            meeting_date=meeting_date,
            term=term,
            agenda=agenda or None,
            minutes=minutes or None,
        )
        db.session.add(mtg)
        db.session.commit()

        flash("Meeting recorded.", "success")
        return redirect(url_for("sms_bp.governance_home"))

    return render_template("subject/sms/governance/meeting_form.html", school=school)

# ---------- LEARNERS ----------

@sms_bp.route("/learners/new", methods=["GET", "POST"])
@login_required
def learners_new():
    school = _current_sms_school()
    if not school:
        flash("Please complete your school setup first.", "warning")
        sms_bp.setup_school

    if request.method == "POST":
        first_name = (request.form.get("first_name") or "").strip()
        last_name  = (request.form.get("last_name")  or "").strip()
        grade      = (request.form.get("grade")      or "").strip()
        class_code = (request.form.get("class_code") or "").strip()
        adm_no     = (request.form.get("admission_no") or "").strip()
        adm_str    = (request.form.get("admission_date") or "").strip()

        if not first_name or not last_name or not grade:
            flash("First name, last name and grade are required.", "danger")
            return redirect(url_for("sms_bp.learners_new"))

        adm_date = None
        if adm_str:
            try:
                y, m, d = [int(p) for p in adm_str.split("-")]  # YYYY-MM-DD
                adm_date = date(y, m, d)
            except Exception:
                flash("Admission date must be YYYY-MM-DD.", "warning")

        lrn = SmsLearner(
            school_id=school.id,
            first_name=first_name,
            last_name=last_name,
            grade=grade,
            class_code=class_code or None,
            admission_no=adm_no or None,
            admission_date=adm_date,
            is_active=True,
        )
        db.session.add(lrn)
        db.session.commit()

        flash("Learner added.", "success")
        return redirect(url_for("sms_bp.learners_home"))

    return render_template("subject/sms/learners/learner_form.html", school=school)

# ---------- TEACHERS ----------

@sms_bp.get("/teachers")
@login_required
def teachers_home():
    school = _current_sms_school()
    if not school:
        flash("Please complete your school setup first.", "warning")
        sms_bp.setup_school

    teachers = (
        SmsTeacher.query
        .filter_by(school_id=school.id, is_active=True)
        .order_by(SmsTeacher.last_name.asc(), SmsTeacher.first_name.asc())
        .all()
    )

    return render_template(
        "subject/sms/teachers/index.html",
        school=school,
        teachers=teachers,
    )


@sms_bp.route("/teachers/new", methods=["GET", "POST"])
@login_required
def teachers_new():
    school = _current_sms_school()
    if not school:
        flash("Please complete your school setup first.", "warning")
        sms_bp.setup_school

    if request.method == "POST":
        first_name = (request.form.get("first_name") or "").strip()
        last_name  = (request.form.get("last_name")  or "").strip()
        employee_no = (request.form.get("employee_no") or "").strip()
        email      = (request.form.get("email") or "").strip()
        phone      = (request.form.get("phone") or "").strip()
        phase      = (request.form.get("phase") or "").strip()
        subjects   = (request.form.get("subjects") or "").strip()

        if not first_name or not last_name:
            flash("First name and last name are required.", "danger")
            return redirect(url_for("sms_bp.teachers_new"))

        t = SmsTeacher(
            school_id=school.id,
            first_name=first_name,
            last_name=last_name,
            employee_no=employee_no or None,
            email=email or None,
            phone=phone or None,
            phase=phase or None,
            subjects=subjects or None,
            is_active=True,
        )
        db.session.add(t)
        db.session.commit()

        flash("Teacher added.", "success")
        return redirect(url_for("sms_bp.teachers_home"))

    return render_template("subject/sms/teachers/teacher_form.html", school=school)

# ---------- MANAGEMENT (SMT) ----------

@sms_bp.get("/management")
@login_required
def management_home():
    school = _current_sms_school()
    if not school:
        flash("Please complete your school setup first.", "warning")
        sms_bp.setup_school

    tasks = (
        SmsMgmtTask.query
        .filter_by(school_id=school.id, is_active=True)
        .order_by(SmsMgmtTask.due_date.asc().nulls_last(), SmsMgmtTask.created_at.desc())
        .all()
    )

    return render_template(
        "subject/sms/management/index.html",
        school=school,
        tasks=tasks,
    )

@sms_bp.route("/management/task/new", methods=["GET", "POST"])
@login_required
def management_task_new():
    school = _current_sms_school()
    if not school:
        flash("Please complete your school setup first.", "warning")
        sms_bp.setup_school

    if request.method == "POST":
        title      = (request.form.get("title") or "").strip()
        owner_name = (request.form.get("owner_name") or "").strip()
        due_str    = (request.form.get("due_date") or "").strip()
        status     = (request.form.get("status") or "open").strip()
        notes      = (request.form.get("notes") or "").strip()

        if not title:
            flash("Title is required.", "danger")
            return redirect(url_for("sms_bp.management_task_new"))

        due_date = None
        if due_str:
            try:
                y, m, d = [int(p) for p in due_str.split("-")]  # YYYY-MM-DD
                due_date = date(y, m, d)
            except Exception:
                flash("Due date must be YYYY-MM-DD.", "warning")

        task = SmsMgmtTask(
            school_id=school.id,
            title=title,
            owner_name=owner_name or None,
            due_date=due_date,
            status=status or "open",
            notes=notes or None,
            is_active=True,
        )
        db.session.add(task)
        db.session.commit()

        flash("Management task added.", "success")
        return redirect(url_for("sms_bp.management_home"))

    return render_template("subject/sms/management/task_form.html", school=school)

# Documentation
@sms_bp.get("/documentation")
@login_required
def documentation_home():
    school = _current_sms_school()
    if not school:
        flash("Please complete your school setup first.", "warning")
        sms_bp.setup_school

    return render_template(
        "subject/sms/documentation/index.html",
        school=school,
    )

@sms_bp.get("/year-progress")
@login_required
def year_progress_home():
    school = _current_sms_school()
    if not school:
        flash("Please complete your school setup first.", "warning")
        sms_bp.setup_school

    return render_template(
        "subject/sms/year_progress/index.html",
        school=school,
    )

@sms_bp.get("/compliance")
@login_required
def compliance_home():
    """Preview page for compliance & communication tools."""
    school = _current_sms_school()
    if not school:
        flash("Please complete your school setup first.", "warning")
        sms_bp.setup_school

    return render_template(
        "subject/sms/compliance/index.html",
        school=school,
    )

@sms_bp.route("/school/profile", methods=["GET", "POST"])
@login_required
def school_profile():
    from flask import request, render_template, redirect, url_for, current_app, flash
    from werkzeug.utils import secure_filename
    import os

    # 1) Get or create the school row for this user
    school = _current_sms_school()
    if not school:
        school = SmsSchool(user_id=current_user.id, name="")
        db.session.add(school)
        db.session.flush()  # get an id

    if request.method == "POST":
        # 2) Basic fields
        school.name     = (request.form.get("name") or "").strip()
        school.emis_no  = (request.form.get("emis_no") or "").strip()
        school.phase    = (request.form.get("phase") or "").strip()
        school.quintile = (request.form.get("quintile") or "").strip()
        learners_raw    = (request.form.get("learners") or "").strip()
        school.learners = int(learners_raw) if learners_raw.isdigit() else None

        # 3) File storage root (instance/school_media by default)
        media_root = current_app.config.get(
            "SMS_MEDIA_ROOT",
            os.path.join(current_app.instance_path, "school_media"),
        )
        os.makedirs(media_root, exist_ok=True)

        def _save_file(field_name, subdir, allowed_ext=None):
            file = request.files.get(field_name)
            if not file or file.filename == "":
                return None

            filename = secure_filename(file.filename)
            ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
            if allowed_ext and ext not in allowed_ext:
                flash(f"Unsupported file type for {field_name}.", "warning")
                return None

            # make per-school folder
            school_dir = os.path.join(media_root, f"school_{school.id}")
            os.makedirs(school_dir, exist_ok=True)

            stored_name = f"{subdir}.{ext}" if ext else subdir
            path = os.path.join(school_dir, stored_name)
            file.save(path)

            # relative path (you can serve from a send_from_directory route)
            rel_path = f"school_media/school_{school.id}/{stored_name}"
            return rel_path

        # 4) Logo upload (optional)
        logo_rel = _save_file("logo_file", "logo", allowed_ext={"png", "jpg", "jpeg"})
        if logo_rel:
            school.logo_path = logo_rel

        # 5) Letterhead upload (PNG/JPG/PDF)
        letter_rel = _save_file(
            "letterhead_file",
            "letterhead",
            allowed_ext={"png", "jpg", "jpeg", "pdf"},
        )
        if letter_rel:
            school.letterhead_path = letter_rel

        db.session.commit()
        flash("School profile updated.", "success")
        return redirect(url_for("sms_bp.school_profile"))

    return render_template("subject/sms/school_profile.html", school=school)

@sms_bp.route("/media/<path:filename>")
@login_required
def school_media(filename):
    import os
    media_root = current_app.config.get(
        "SMS_MEDIA_ROOT",
        os.path.join(current_app.instance_path, "school_media"),
    )
    return send_from_directory(media_root, filename, as_attachment=False)

@sms_bp.route("/learners", methods=["GET"])
@login_required
def learners_home():
    """List learners for the current user's school."""
    school = _current_sms_school()
    if not school:
        flash("Please set up your school profile first.", "warning")
        return redirect(url_for("sms_bp.setup_school"))

    # Load learners with their primary guardian (if any)
    learners = (
        db.session.query(SmsLearner)
        .filter(SmsLearner.school_id == school.id)
        .order_by(SmsLearner.last_name, SmsLearner.first_name)
        .all()
    )

    # Map learner_id -> primary guardian (if any)
    # Map learner_id -> primary guardian link (with relationship + contact)
    primary_guardians = {}
    if learners:
        learner_ids = [l.id for l in learners]
        links = (
            db.session.query(SmsLearnerGuardian)
            .join(SmsGuardian)
            .filter(SmsLearnerGuardian.learner_id.in_(learner_ids))
            .order_by(
                SmsLearnerGuardian.learner_id,
                SmsLearnerGuardian.is_primary.desc(),
                SmsGuardian.full_name,
            )
            .all()
        )
        for link in links:
            # first one per learner wins (is_primary first because of order_by)
            if link.learner_id not in primary_guardians:
                primary_guardians[link.learner_id] = link


    return render_template(
        "subject/sms/learners/learners_home.html",
        learners=learners,
        primary_guardians=primary_guardians,
        school=school,
    )

@sms_bp.route("/learners/add", methods=["GET", "POST"])
@login_required
def learners_add():
    """Create a learner + primary guardian in one form."""
    school = _current_sms_school()
    if not school:
        flash("Please set up your school profile first.", "warning")
        return redirect(url_for("sms_bp.setup_school"))

    if request.method == "POST":
        # --- learner fields ---
        first_name   = (request.form.get("first_name") or "").strip()
        last_name    = (request.form.get("last_name") or "").strip()
        grade        = (request.form.get("grade") or "").strip()
        class_code   = (request.form.get("class_code") or "").strip()
        admission_no_raw = (request.form.get("admission_no") or "").strip()
        admission_no = admission_no_raw or None
        admission_dt = (request.form.get("admission_date") or "").strip()

        # --- guardian fields ---
        g_name   = (request.form.get("guardian_name") or "").strip()
        g_rel    = (request.form.get("guardian_relationship") or "").strip()
        g_email  = (request.form.get("guardian_email") or "").strip()
        g_phone  = (request.form.get("guardian_phone") or "").strip()

        # Basic validation
        if not first_name or not last_name or not grade:
            flash("Please enter learner first name, last name and grade.", "warning")
            return redirect(url_for("sms_bp.learners_add"))

        if not g_name:
            flash("Please enter at least one parent / guardian name.", "warning")
            return redirect(url_for("sms_bp.learners_add"))

        # Parse admission date
        adm_date_obj = None
        if admission_dt:
            try:
                adm_date_obj = datetime.strptime(admission_dt, "%Y-%m-%d").date()
            except ValueError:
                flash("Admission date must be in format YYYY-MM-DD.", "warning")
                return redirect(url_for("sms_bp.learners_add"))

        # Enforce per-school uniqueness of admission number
        if admission_no:
            existing = (
                SmsLearner.query
                .filter_by(school_id=school.id, admission_no=admission_no)
                .first()
            )
            if existing:
                flash("A learner with that admission number already exists for this school.", "warning")
                return redirect(url_for("sms_bp.learners_add"))

        # --- create learner ---
        learner = SmsLearner(
            school_id=school.id,
            first_name=first_name,
            last_name=last_name,
            grade=grade,
            class_code=class_code or None,
            admission_no=admission_no,
            admission_date=adm_date_obj,
            is_active=True,
        )
        db.session.add(learner)
        db.session.flush()  # get learner.id

        # --- find or create guardian ---
        guardian = None
        if g_email:
            guardian = SmsGuardian.query.filter(
                func.lower(SmsGuardian.email) == g_email.lower(),
                SmsGuardian.school_id == school.id,
            ).first()

        if guardian is None:
            # fallback match on name + phone
            guardian_q = SmsGuardian.query.filter(
                SmsGuardian.school_id == school.id,
                func.lower(SmsGuardian.full_name) == g_name.lower(),
            )
            if g_phone:
                guardian_q = guardian_q.filter(SmsGuardian.phone == g_phone)
            guardian = guardian_q.first()

        if guardian is None:
            guardian = SmsGuardian(
                school_id=school.id,
                full_name=g_name,
                relationship=g_rel or None,
                email=g_email or None,
                phone=g_phone or None,
            )
            db.session.add(guardian)
            db.session.flush()

        # --- link learner ↔ guardian ---
        link = SmsLearnerGuardian(
            learner_id=learner.id,
            guardian_id=guardian.id,
            is_primary=True,
        )
        db.session.add(link)

        db.session.commit()
        flash("Learner and primary guardian added.", "success")
        return redirect(url_for("sms_bp.learners_home"))

    return render_template(
        "subject/sms/learners/learners_add.html",
        school=school,
    )

@sms_bp.route("/learners/<int:learner_id>/parents", methods=["GET", "POST"])
@login_required
def learner_parents(learner_id):
    from flask import abort

    # Ensure the user has a school
    school = _current_sms_school()
    if not school:
        flash("Please set up your school profile first.", "warning")
        return redirect(url_for("sms_bp.setup_school"))

    # Ensure the learner belongs to this school
    learner = (
        SmsLearner.query
        .filter_by(id=learner_id, school_id=school.id)
        .first()
    )
    if not learner:
        abort(404)

    # Existing links for this learner
    links = (
        db.session.query(SmsLearnerGuardian)
        .join(SmsGuardian)
        .filter(SmsLearnerGuardian.learner_id == learner.id)
        .all()
    )

    # role -> SmsGuardian (mother / father / guardian)
    guardians_by_role: dict[str, SmsGuardian] = {}
    for link in links:
        guardian = link.guardian
        if not guardian:
            continue
        role = (guardian.relationship or "").lower()
        if role in ("mother", "father", "guardian"):
            guardians_by_role[role] = guardian

    if request.method == "POST":
        roles = ("mother", "father", "guardian")

        for role in roles:
            full_name = (request.form.get(f"{role}_name") or "").strip()
            email = (request.form.get(f"{role}_email") or "").strip()
            phone = (request.form.get(f"{role}_phone") or "").strip()

            # If no name, skip this role entirely
            if not full_name:
                continue

            guardian = guardians_by_role.get(role)
            if guardian:
                # Update existing guardian
                guardian.full_name = full_name
                guardian.email = email or None
                guardian.phone = phone or None
                guardian.relationship = role
            else:
                # Create new guardian
                guardian = SmsGuardian(
                    school_id=school.id,
                    full_name=full_name,
                    email=email or None,
                    phone=phone or None,
                    relationship=role,
                )
                db.session.add(guardian)
                db.session.flush()  # ensure guardian.id

                # Link learner -> guardian (no relationship field here)
                link = SmsLearnerGuardian(
                    learner_id=learner.id,
                    guardian_id=guardian.id,
                    is_primary=(role == "mother"),
                )
                db.session.add(link)

        db.session.commit()
        flash("Parent / guardian details updated.", "success")
        return redirect(url_for("sms_bp.learners_home"))

    return render_template(
        "subject/sms/learners/learner_parents.html",
        school=school,
        learner=learner,
        mother=guardians_by_role.get("mother"),
        father=guardians_by_role.get("father"),
        guardian=guardians_by_role.get("guardian"),
    )

@sms_bp.get("/finance")
@login_required
def finance_home():
    if not has_sms_finance_access():
        abort(403)

    school = _current_sms_school()
    if not school:
        flash("Please complete your school setup first.", "warning")
        return redirect(url_for("sms_bp.setup_school"))


    # Stub for now – we’ll add real fee / asset data later
    fee_summary = {
        "year": None,
        "terms": [],
    }
    asset_summary = {
        "categories": [],
    }

    return render_template(
        "subject/sms/finance/index.html",
        school=school,
        fee_summary=fee_summary,
        asset_summary=asset_summary,
    )

@sms_bp.route("/finance/categories", methods=["GET", "POST"])
@login_required
def finance_categories():
    if not has_sms_finance_access():
        abort(403)

    school = _current_sms_school()
    if not school:
        flash("Please set up your school profile first.", "warning")
        return redirect(url_for("sms_bp.setup_school"))

    # remember where we came from (e.g., next=cashbook)
    next_page = request.args.get("next") or request.form.get("next") or ""

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        kind = (request.form.get("kind") or "").strip().lower()  # income/expense

        if not name or kind not in ("income", "expense"):
            flash("Please enter a name and choose income or expense.", "warning")
        else:
            cat = SmsFinCategory(
                school_id=school.id,
                name=name,
                kind=kind,
            )
            db.session.add(cat)
            db.session.commit()
            flash("Category added.", "success")

            # return to cashbook if called from there
            if next_page == "cashbook":
                return redirect(url_for("sms_bp.finance_transactions"))

            return redirect(url_for("sms_bp.finance_categories"))

    categories = (
        SmsFinCategory.query
        .filter_by(school_id=school.id)
        .order_by(SmsFinCategory.kind.asc(), SmsFinCategory.name.asc())
        .all()
    )

    return render_template(
        "subject/sms/finance/categories.html",
        school=school,
        categories=categories,
        next_page=next_page,
    )

@sms_bp.route("/finance/summary", methods=["GET"])
@login_required
def finance_summary():
    if not has_sms_finance_access():
        abort(403)

    """Income vs expenses + simple fee planning helper."""
    school = _current_sms_school()
    if not school:
        flash("Please set up your school profile first.", "warning")
        return redirect(url_for("sms_bp.setup_school"))

    year = int(request.args.get("year") or datetime.now().year)

    start = date(year, 1, 1)
    end = date(year, 12, 31)

    txns = (
        SmsFinTxn.query
        .filter(
            SmsFinTxn.school_id == school.id,
            SmsFinTxn.date >= start,
            SmsFinTxn.date <= end,
        )
        .join(SmsFinCategory, SmsFinCategory.id == SmsFinTxn.category_id)
        .add_entity(SmsFinCategory)
        .all()
    )

    income_total = 0
    expense_total = 0
    by_category = {}

    for txn, cat in txns:
        signed = txn.amount_cents if txn.direction == "in" else -txn.amount_cents

        key = cat.name
        by_category.setdefault(key, 0)
        by_category[key] += signed

        if txn.direction == "in":
            income_total += txn.amount_cents
        else:
            expense_total += txn.amount_cents

    surplus_cents = income_total - expense_total

    # fee planning: very simple helper
    expected_learners = SmsLearner.query.filter_by(
        school_id=school.id,
        is_active=True
    ).count()

    # Non-fee income (hall hire, subsidy, etc.)
    fee_cats = SmsFinCategory.query.filter_by(
        school_id=school.id,
        is_fee=True,
        kind="income",
    ).all()
    fee_cat_ids = {c.id for c in fee_cats}

    fee_income_cents = 0
    non_fee_income_cents = 0
    for txn, cat in txns:
        if txn.direction != "in":
            continue
        if txn.category_id in fee_cat_ids:
            fee_income_cents += txn.amount_cents
        else:
            non_fee_income_cents += txn.amount_cents

    required_fee_per_learner_cents = None
    if expected_learners > 0:
        # target: cover expenses entirely from fee income + non-fee income
        required_fee_income_cents = max(
            expense_total - non_fee_income_cents,
            0,
        )
        required_fee_per_learner_cents = int(
            round(required_fee_income_cents / expected_learners)
        )

    return render_template(
        "subject/sms/finance/summary.html",
        school=school,
        year=year,
        income_total=income_total,
        expense_total=expense_total,
        surplus_cents=surplus_cents,
        by_category=by_category,
        expected_learners=expected_learners,
        required_fee_per_learner_cents=required_fee_per_learner_cents,
    )

@sms_bp.route("/finance/transactions", methods=["GET", "POST"])
@login_required
def finance_transactions():
    if not has_sms_finance_access():
        abort(403)

    """Simple cashbook: record all income and expenses."""
    from datetime import datetime

    school = _current_sms_school()
    if not school:
        flash("Please set up your school profile first.", "warning")
        return redirect(url_for("sms_bp.setup_school"))

    # --- lookups for form ---
    categories = (
        SmsFinCategory.query
        .filter_by(school_id=school.id)
        .order_by(SmsFinCategory.kind.desc(), SmsFinCategory.name)
        .all()
    )

    learners = (
        SmsLearner.query
        .filter_by(school_id=school.id, is_active=True)
        .order_by(
            SmsLearner.grade,
            SmsLearner.class_code,
            SmsLearner.last_name,
            SmsLearner.first_name,
        )
        .all()
    )

    if request.method == "POST":
        date_str    = (request.form.get("date") or "").strip()
        category_id = (request.form.get("category_id") or "").strip()
        amount_str  = (request.form.get("amount") or "").strip()
        learner_id  = (request.form.get("learner_id") or "").strip() or None
        # description is no longer on the form, will be "", that’s fine
        description = (request.form.get("description") or "").strip()
        method      = (request.form.get("method") or "").strip()  # cash, eft, card, petty_cash, bank_withdrawal, other
        bank_ref    = (request.form.get("bank_ref") or "").strip()

        # --- date ---
        try:
            date_val = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            flash("Please enter a valid date (YYYY-MM-DD).", "warning")
            return redirect(url_for("sms_bp.finance_transactions"))

        # --- category ---
        if not category_id:
            flash("Please choose a category.", "warning")
            return redirect(url_for("sms_bp.finance_transactions"))

        cat = SmsFinCategory.query.filter_by(
            id=category_id,
            school_id=school.id,
        ).first()

        if not cat:
            flash("Selected category is not valid.", "warning")
            return redirect(url_for("sms_bp.finance_transactions"))

        if cat.kind not in ("income", "expense"):
            flash("Category kind must be income or expense.", "warning")
            return redirect(url_for("sms_bp.finance_transactions"))

        direction = "in" if cat.kind == "income" else "out"

        # Is this effectively “fee income”?
        is_fee_like = bool(
            cat.is_fee or cat.name.lower().startswith("school fee")
        )

        # --- amount ---
        try:
            rands = float(amount_str)
            amount_cents = int(round(rands * 100))
        except ValueError:
            flash("Please enter a valid amount.", "warning")
            return redirect(url_for("sms_bp.finance_transactions"))

        # --- learner (for fee income) ---
        learner = None
        if learner_id:
            learner = SmsLearner.query.filter_by(
                id=learner_id,
                school_id=school.id,
            ).first()

        if is_fee_like and not learner:
            flash("Please select the learner for this school fee payment.", "warning")
            return redirect(url_for("sms_bp.finance_transactions"))

        # --- CASH / PETTY CASH RECEIPT / VOUCHER TRACKING ---
        if method in ("cash", "petty_cash"):
            label = "receipt" if method == "cash" else "voucher"

            if not bank_ref:
                flash(f"Please enter the {label} number for {label} payments.", "warning")
                return redirect(url_for("sms_bp.finance_transactions"))

            if not bank_ref.isdigit():
                flash(f"{label.capitalize()} number must be digits only (no spaces).", "warning")
                return redirect(url_for("sms_bp.finance_transactions"))

            num_val = int(bank_ref)

            # Track each series separately (cash vs petty cash)
            last_txn = (
                SmsFinTxn.query
                .filter_by(school_id=school.id, method=method)
                .order_by(SmsFinTxn.date.desc(), SmsFinTxn.id.desc())
                .first()
            )
            last_num = None
            if last_txn and last_txn.bank_ref and last_txn.bank_ref.isdigit():
                last_num = int(last_txn.bank_ref)

            # Auditor-only flag if number goes backwards / repeats
            if last_num is not None and num_val <= last_num:
                flash(
                    f"{label.title()} sequence issue: {num_val} is not higher than previous {last_num}.",
                    "audit",
                )

            if last_num is not None and num_val > last_num + 1:
                missing_from = last_num + 1
                missing_to = num_val - 1
                gap_msg = f"{missing_from}" if missing_from == missing_to else f"{missing_from}–{missing_to}"
                flash(
                    f"Missing {label}(s): {gap_msg} between {last_num} and {num_val}.",
                    "audit",
                )




        # --- BUILD DESCRIPTION ---
        extra_tags = []

        # tag for method + ref (for all methods)
        if method == "cash" and bank_ref:
            extra_tags.append(f"cash rcpt {bank_ref}")
        elif method == "petty_cash" and bank_ref:
            extra_tags.append(f"petty cash v {bank_ref}")
        elif method == "eft" and bank_ref:
            extra_tags.append(f"EFT {bank_ref}")
        elif method == "bank_withdrawal" and bank_ref:
            extra_tags.append(f"bank wd {bank_ref}")
        elif bank_ref:
            extra_tags.append(bank_ref)

        # auto-base for fee-like rows
        if is_fee_like and learner:
            base_desc = (
                f"School fees – {learner.grade}"
                f"{(' ' + (learner.class_code or '')).strip()}"
                f" – {learner.last_name}, {learner.first_name}"
            )
            if description:
                base_desc = f"{base_desc} – {description}"
        else:
            # non-fee categories: at least show category name if user left blank
            base_desc = description or cat.name

        if extra_tags:
            base_desc = f"{base_desc} ({', '.join(extra_tags)})"

        final_desc = base_desc

        # --- create transaction ---
        txn = SmsFinTxn(
            school_id=school.id,
            date=date_val,
            amount_cents=amount_cents,
            direction=direction,
            category_id=cat.id,
            learner_id=learner.id if learner else None,
            description=final_desc,
            method=method or None,
            bank_ref=bank_ref or None,
        )
        db.session.add(txn)
        db.session.commit()
        flash("Transaction recorded.", "success")
        return redirect(url_for("sms_bp.finance_transactions"))

    # --- recent transactions for display (ledger style: oldest first) ---
    recent = (
        SmsFinTxn.query
        .filter_by(school_id=school.id)
        .order_by(SmsFinTxn.date.desc(), SmsFinTxn.id.desc())
        .limit(50)
        .all()
    )


    return render_template(
        "subject/sms/finance/transactions.html",
        school=school,
        categories=categories,
        learners=learners,
        transactions=recent,
    )

@sms_bp.app_context_processor
def _sms_nav_ctx():
    return {"has_sms_audit_nav_access": has_sms_audit_access}


@sms_bp.get("/audit/findings")
@login_required
def audit_findings():
    # auditor-only (no owner)
    school = _require_sms_auditor_school()

    def _audit_for_methods(methods, label):
        txns = (
            SmsFinTxn.query
            .filter(SmsFinTxn.school_id == school.id)
            .filter(SmsFinTxn.method.in_(methods))
            .order_by(SmsFinTxn.date.asc(), SmsFinTxn.id.asc())
            .all()
        )

        items = []
        for t in txns:
            ref = (t.bank_ref or "").strip()
            if not ref.isdigit():
                continue
            items.append({"txn_id": t.id, "date": t.date, "ref": int(ref)})

        findings = []
        last_ref = None
        last_txn_id = None

        for it in items:
            cur = it["ref"]

            if last_ref is not None:
                if cur <= last_ref:
                    findings.append({
                        "type": "reversal",
                        "label": label,
                        "message": f"{label} number {cur} is not higher than previous {last_ref}.",
                        "from_ref": last_ref,
                        "to_ref": cur,
                        "txn_id": it["txn_id"],
                        "prev_txn_id": last_txn_id,
                        "date": it["date"],
                    })
                elif cur > last_ref + 1:
                    findings.append({
                        "type": "gap",
                        "label": label,
                        "message": f"Missing {label}(s): {last_ref+1}–{cur-1} between {last_ref} and {cur}.",
                        "from_ref": last_ref + 1,
                        "to_ref": cur - 1,
                        "txn_id": it["txn_id"],
                        "prev_txn_id": last_txn_id,
                        "date": it["date"],
                    })

            last_ref = cur
            last_txn_id = it["txn_id"]

        summary = {
            "label": label,
            "count": len(items),
            "first": items[0]["ref"] if items else None,
            "last": items[-1]["ref"] if items else None,
            "gaps": sum(1 for f in findings if f["type"] == "gap"),
            "reversals": sum(1 for f in findings if f["type"] == "reversal"),
        }

        return summary, findings, items[-25:]

    books = [
        _audit_for_methods(["cash"], "Receipt"),
        _audit_for_methods(["petty_cash"], "Voucher"),
        _audit_for_methods(["eft"], "EFT ref"),
    ]

    summaries = [b[0] for b in books]
    findings = [f for b in books for f in b[1]]
    findings.sort(key=lambda x: (x["date"], x["txn_id"]), reverse=True)
    recent_refs = [{"summary": b[0], "recent": b[2]} for b in books]

    return render_template(
        "subject/sms/audit/findings.html",
        school=school,
        summaries=summaries,
        findings=findings,
        recent_refs=recent_refs,
    )


@sms_bp.route("/finance/audit/access", methods=["GET", "POST"])
@login_required
def finance_audit_access():
    # Owner ONLY can grant roles
    school = _sms_owner_school()
    if not school:
        abort(403)

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()

        if action == "add":
            email = (request.form.get("email") or "").strip().lower()
            role = (request.form.get("role") or "").strip().lower()

            if not email or role not in ("auditor", "sgb"):
                flash("Enter an email and choose a role.", "warning")
            else:
                # Avoid ORM (your model has created_by_user_id but DB doesn't)
                existing = db.session.execute(
                    text("""
                        SELECT id, active
                          FROM sms_approved_user
                         WHERE school_id = :sid
                           AND email = :email
                           AND role = :role
                         LIMIT 1
                    """),
                    {"sid": school.id, "email": email, "role": role},
                ).mappings().first()

                if existing:
                    db.session.execute(
                        text("""
                            UPDATE sms_approved_user
                               SET active = true
                             WHERE id = :id
                        """),
                        {"id": existing["id"]},
                    )
                else:
                    db.session.execute(
                        text("""
                            INSERT INTO sms_approved_user (school_id, email, role, active)
                            VALUES (:sid, :email, :role, true)
                        """),
                        {"sid": school.id, "email": email, "role": role},
                    )

                db.session.commit()
                flash("Access saved.", "success")

        elif action == "deactivate":
            row_id = request.form.get("row_id", type=int)
            if row_id:
                db.session.execute(
                    text("""
                        UPDATE sms_approved_user
                           SET active = false
                         WHERE id = :id
                           AND school_id = :sid
                    """),
                    {"id": row_id, "sid": school.id},
                )
                db.session.commit()
                flash("Access removed.", "success")

        return redirect(url_for("sms_bp.finance_audit_access"))

    approved_rows = db.session.execute(
        text("""
            SELECT id, role, email, active
              FROM sms_approved_user
             WHERE school_id = :sid
             ORDER BY role ASC, email ASC
        """),
        {"sid": school.id},
    ).mappings().all()

    # make template keep working with dot access (a.role, a.email, etc.)
    approved = [SimpleNamespace(**r) for r in approved_rows]

    return render_template(
        "subject/sms/audit/access.html",   # NEW location
        school=school,
        approved=approved,
    )

@sms_bp.route("/finance/audit", methods=["GET"])
@login_required
def finance_audit():
    # Integrity rule: owner/principal is NOT allowed into audit findings
    if _sms_owner_school():
        abort(403)

    email = (getattr(current_user, "email", "") or "").strip().lower()
    if not email:
        abort(403)

    # Must be an ACTIVE auditor for some school
    row = db.session.execute(
        text("""
            SELECT school_id
              FROM sms_approved_user
             WHERE email = :email
               AND role = 'auditor'
               AND active = true
             ORDER BY id DESC
             LIMIT 1
        """),
        {"email": email},
    ).mappings().first()

    if not row:
        abort(403)

    school = db.session.get(SmsSchool, row["school_id"])
    if not school:
        abort(403)

    def _audit_for_methods(methods, label):
        txns = (
            SmsFinTxn.query
            .filter(SmsFinTxn.school_id == school.id)
            .filter(SmsFinTxn.method.in_(methods))
            .order_by(SmsFinTxn.date.asc(), SmsFinTxn.id.asc())
            .all()
        )

        items = []
        for t in txns:
            ref = (t.bank_ref or "").strip()
            if not ref.isdigit():
                continue
            items.append({"txn_id": t.id, "date": t.date, "ref": int(ref)})

        findings = []
        last_ref = None
        last_txn_id = None

        for it in items:
            cur = it["ref"]

            if last_ref is not None:
                if cur <= last_ref:
                    findings.append({
                        "type": "reversal",
                        "label": label,
                        "message": f"{label} number {cur} is not higher than previous {last_ref}.",
                        "from_ref": last_ref,
                        "to_ref": cur,
                        "txn_id": it["txn_id"],
                        "prev_txn_id": last_txn_id,
                        "date": it["date"],
                    })
                elif cur > last_ref + 1:
                    findings.append({
                        "type": "gap",
                        "label": label,
                        "message": f"Missing {label}(s): {last_ref+1}–{cur-1} between {last_ref} and {cur}.",
                        "from_ref": last_ref + 1,
                        "to_ref": cur - 1,
                        "txn_id": it["txn_id"],
                        "prev_txn_id": last_txn_id,
                        "date": it["date"],
                    })

            last_ref = cur
            last_txn_id = it["txn_id"]

        summary = {
            "label": label,
            "count": len(items),
            "first": items[0]["ref"] if items else None,
            "last": items[-1]["ref"] if items else None,
            "gaps": sum(1 for f in findings if f["type"] == "gap"),
            "reversals": sum(1 for f in findings if f["type"] == "reversal"),
        }
        return summary, findings, items[-25:]

    books = [
        _audit_for_methods(["cash"], "Receipt"),
        _audit_for_methods(["petty_cash"], "Voucher"),
        _audit_for_methods(["eft"], "EFT ref"),
    ]

    summaries = [b[0] for b in books]
    findings = [f for b in books for f in b[1]]
    findings.sort(key=lambda x: (x["date"], x["txn_id"]), reverse=True)
    recent_refs = [{"summary": b[0], "recent": b[2]} for b in books]

    return render_template(
        "subject/sms/audit/findings.html",   # NEW location
        school=school,
        summaries=summaries,
        findings=findings,
        recent_refs=recent_refs,
    )

@sms_bp.get("/audit")
@login_required
def audit_dashboard():
    if not has_sms_role("auditor"):
        abort(403)

    email = (getattr(current_user, "email", "") or "").strip().lower()
    approval = (
        SmsApprovedUser.query
        .filter_by(email=email, active=True, role="auditor")
        .order_by(SmsApprovedUser.id.desc())
        .first()
    )
    if not approval:
        abort(403)

    school = db.session.get(SmsSchool, approval.school_id)
    if not school:
        return redirect(url_for("sms_bp.no_access"))

    return render_template("subject/sms/audit/dashboard.html", school=school)

@sms_bp.get("/setup-dashboard", endpoint="setup_dashboard")
@sms_bp.post("/setup-dashboard", endpoint="setup_dashboard_post")
@login_required
def setup_dashboard():
    school = _sms_owner_school()
    if not school:
        abort(403)

    # People sources
    teachers = (
        SmsTeacher.query
        .filter_by(school_id=school.id, is_active=True)
        .order_by(SmsTeacher.last_name.asc(), SmsTeacher.first_name.asc())
        .all()
    )

    # ✅ Roles are DB-driven (no hardcoding)
    roles = (
        SmsRole.query
        .filter_by(active=True)
        .order_by(SmsRole.sort_order.asc(), SmsRole.label.asc())
        .all()
    )
    allowed_roles = {r.code for r in roles}

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()

        if action == "add_access":
            role = (request.form.get("role") or "").strip().lower()
            email = (request.form.get("email") or "").strip().lower()

            person_type = (request.form.get("person_type") or "").strip().lower()
            person_id = (request.form.get("person_id") or "").strip()

            # If they chose a person, that wins (email is derived from DB)
            if person_type == "teacher" and person_id.isdigit():
                t = SmsTeacher.query.filter_by(
                    id=int(person_id),
                    school_id=school.id,
                    is_active=True
                ).first()
                if t and t.email:
                    email = t.email.strip().lower()

            if role not in allowed_roles:
                flash("That role is not available. Add/activate it in Roles first.", "warning")
                return redirect(url_for("sms_bp.setup_dashboard"))

            if not email:
                flash("Select a person (or enter an email).", "warning")
                return redirect(url_for("sms_bp.setup_dashboard"))

            row = SmsApprovedUser.query.filter_by(
                school_id=school.id, email=email, role=role
            ).first()

            if row:
                row.active = True
            else:
                db.session.add(SmsApprovedUser(
                    school_id=school.id,
                    email=email,
                    role=role,
                    active=True,
                    created_by_user_id=current_user.id,
                ))

            try:
                db.session.commit()
                flash("Access saved.", "success")
            except Exception:
                db.session.rollback()
                flash("Could not save access. (Role/email rejected by database rules.)", "danger")

        elif action == "toggle_access":
            row_id = request.form.get("row_id", type=int)
            row = SmsApprovedUser.query.filter_by(id=row_id, school_id=school.id).first()
            if row:
                row.active = not bool(row.active)
                db.session.commit()
                flash("Access updated.", "success")

        return redirect(url_for("sms_bp.setup_dashboard"))

    approved = (
        SmsApprovedUser.query
        .filter_by(school_id=school.id)
        .order_by(SmsApprovedUser.role.asc(), SmsApprovedUser.email.asc())
        .all()
    )

    return render_template(
        "subject/sms/setup_dashboard.html",
        school=school,
        approved=approved,
        teachers=teachers,
        roles=roles,  # ✅ pass to template
    )

@sms_bp.get("/setup/people", endpoint="setup_people")
@login_required
def setup_people():
    # principal-only, but return JSON (not HTML abort) so the UI can handle it
    school = _sms_owner_school()
    if not school:
        return jsonify([]), 200

    ptype = (request.args.get("type") or "").strip().lower()
    out = []

    if ptype == "teacher":
        rows = (
            SmsTeacher.query
            .filter_by(school_id=school.id, is_active=True)
            .order_by(SmsTeacher.last_name.asc(), SmsTeacher.first_name.asc())
            .all()
        )

        for t in rows:
            fn = (t.first_name or "").strip()
            ln = (t.last_name or "").strip()
            email = (getattr(t, "email", "") or "").strip().lower()

            label = ", ".join([x for x in [ln, fn] if x]) or "(Unnamed teacher)"
            if email:
                label = f"{label} ({email})"

            out.append({"id": int(t.id), "label": label, "email": email})

    return jsonify(out), 200

@sms_bp.get("/assigned-role/<role>", endpoint="_route_role")
@login_required
def _route_role(role):
    role = (role or "").strip().lower()

    # IMPORTANT: auditor-only audit
    if role == "auditor":
        return redirect(url_for("sms_bp.finance_audit"))

    if role in ("sgb", "treasurer"):
        return redirect(url_for("sms_bp.finance_home"))

    return redirect(url_for("sms_bp.no_access"))

@sms_bp.route("/assigned-role", methods=["GET", "POST"])
def assigned_role():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        if not email:
            flash("Enter your email.", "warning")
            return redirect(url_for("sms_bp.assigned_role"))

        approvals = (
            SmsApprovedUser.query
            .filter_by(email=email, active=True)
            .order_by(SmsApprovedUser.id.desc())
            .all()
        )
        if not approvals:
            flash("No active role found for that email.", "danger")
            return redirect(url_for("sms_bp.assigned_role"))

        session["sms_role_email"] = email
        return redirect(url_for("sms_bp.assigned_role_confirm"))

    return render_template("subject/sms/assigned_role.html")

@sms_bp.post("/assigned-role/continue")
def assigned_role_continue():
    email = (session.get("sms_role_email") or "").strip().lower()
    if not email:
        return redirect(url_for("sms_bp.assigned_role"))

    # must still be approved
    ok = SmsApprovedUser.query.filter_by(email=email, active=True).first()
    if not ok:
        flash("No active role found for that email.", "danger")
        return redirect(url_for("sms_bp.assigned_role"))

    user = User.query.filter_by(email=email).first()

    if user:
        # already registered → normal login (prefill email) then route via sieve
        return redirect(url_for("auth_bp.login", email=email, next=url_for("sms_bp.sms_entry")))

    # not registered → SMS quick-register (email locked)
    session["sms_quickreg_email"] = email
    return redirect(url_for("sms_bp.sms_quick_register"))

@sms_bp.route("/assigned-role/confirm", methods=["GET", "POST"])
def assigned_role_confirm():
    # If someone is already logged in, don't allow re-claiming via assigned-role
    if getattr(current_user, "is_authenticated", False):
        return redirect(url_for("sms_bp.sms_entry"))

    email = (session.get("sms_role_email") or "").strip().lower()
    if not email:
        return redirect(url_for("sms_bp.assigned_role"))

    approvals = (
        SmsApprovedUser.query
        .filter_by(email=email, active=True)
        .order_by(SmsApprovedUser.id.desc())
        .all()
    )
    if not approvals:
        session.pop("sms_role_email", None)
        flash("No active role found for that email.", "danger")
        return redirect(url_for("sms_bp.assigned_role"))

    newest = approvals[0]
    school = db.session.get(SmsSchool, newest.school_id)
    if not school:
        session.pop("sms_role_email", None)
        flash("That role is linked to a school that no longer exists.", "danger")
        return redirect(url_for("sms_bp.assigned_role"))

    roles = sorted({a.role for a in approvals})
    role_primary = newest.role

    # user exists?
    from app.models import User
    user_exists = bool(User.query.filter(User.email.ilike(email)).first())

    if request.method == "POST":
        if user_exists:
            # ✅ existing user → go to normal login aware of email + redirect target
            return redirect(url_for("auth_bp.login", email=email, next=url_for("sms_bp.sms_entry")))
        # ✅ new user → abridged register (email locked, password only)
        return redirect(url_for("sms_bp.assigned_role_register"))

    return render_template(
        "subject/sms/assigned_role_confirm.html",
        email=email,
        roles=roles,
        role_primary=role_primary,
        school=school,
        user_exists=user_exists,
    )

@sms_bp.get("/assigned-role/register")
@sms_bp.post("/assigned-role/register")
def sms_quick_register():
    email = (session.get("sms_quickreg_email") or "").strip().lower()
    if not email:
        return redirect(url_for("sms_bp.assigned_role"))

    ok = SmsApprovedUser.query.filter_by(email=email, active=True).first()
    if not ok:
        session.pop("sms_quickreg_email", None)
        flash("No active role found for that email.", "danger")
        return redirect(url_for("sms_bp.assigned_role"))

    u = User.query.filter(User.email.ilike(email)).first()
    has_pw = bool(getattr(u, "password_hash", None)) if u else False

    # If user already has a password -> normal login
    if u and has_pw:
        return redirect(url_for("auth_bp.login", email=email, next=url_for("sms_bp.sms_entry")))

    if request.method == "POST":
        pw = (request.form.get("password") or "")
        pw2 = (request.form.get("password2") or "")

        if not pw or pw != pw2 or len(pw) < 8:
            flash("Enter a password (min 8 chars) and confirm it.", "warning")
            return redirect(url_for("sms_bp.sms_quick_register"))

        # Create user if missing, otherwise set password on existing user-without-password
        if not u:
            u = User(email=email)
            db.session.add(u)

    

        u.password_hash = generate_password_hash(pw)
        db.session.commit()


        login_user(u)
        session.pop("sms_quickreg_email", None)
        return redirect(url_for("sms_bp.sms_entry"))

    return render_template("subject/sms/assigned_role_register.html", email=email)

@sms_bp.get("/entry")
@login_required
def sms_entry():
    # 1) resolve email
    email = (getattr(current_user, "email", "") or "").strip().lower()
    if not email:
        return redirect(url_for("sms_bp.no_access"))

    # 2) If this email has an active approved role, route by role FIRST (before owner-school logic)
    approvals = (
        SmsApprovedUser.query
        .filter_by(email=email, active=True)
        .order_by(SmsApprovedUser.id.desc())
        .all()
    )
    roles = {a.role for a in approvals}

    if "auditor" in roles:
        return redirect(url_for("sms_bp.audit_dashboard"))   # your audit dashboard
    if "teacher" in roles:
        return redirect(url_for("sms_bp.teachers_home"))     # until a teacher dashboard exists
    if "hod" in roles or "dp" in roles:
        return redirect(url_for("sms_bp.management_home"))
    if "sgb" in roles or "treasurer" in roles:
        return redirect(url_for("sms_bp.finance_home"))

    # 3) No approved role → then treat as owner/principal flow
    school = _sms_owner_school()
    if school:
        if (school.name or "").strip():
            return redirect(url_for("sms_bp.subject_home"))
        return redirect(url_for("sms_bp.setup_dashboard"))

    return redirect(url_for("sms_bp.no_access"))
