# app/program_culturefire/routes.py
from sqlalchemy.orm import joinedload
from flask import (
    Blueprint, abort, app, current_app, flash, jsonify, redirect, 
    render_template, request, send_from_directory, session, url_for
    )
from flask_login import current_user, login_required
from wtforms import SelectField
from app.forms import (
    BiodataForm, EnrollmentStep1Form, EnrollmentStep2Form, 
    EnrollmentStep3Form, NewGroupForm, PageantForm, 
    ParentAddParticipantForm,  PermissionForm, SegmentSelectForm, ShowcaseForm, 
    SponsorForm, SupporterForm, TalentDetailsForm, TalentForm, TalentSubmissionForm, 
    UpdateGroupForm
    )
from app.models.auth import AuthSubject, User, UserEnrollment, UserRole
from app.extensions import db

from app.models.culturalfire import (
    CfiBiodata, CfiGroup, CfiGroupMember, CfiPageantSegment, CfiParent, CfiRole, CfiSegmentItem, CfiShow, CfiSponsorItem, CfiSponsorship, CfiSubmissionParticipant, 
    CfiSupporter, CfiTalentCategoryItem, CfiTalentContext, CfiTalentFile, CfiTalentStyle, CfiShowcaseVote, 
    CfiTalentSubmission, CfiJudgeAssignment
    )
from app.program_culturalfire.helpers import (
    all_segments_filled,
    auto_generate_show_from_submissions,
    build_filename,
    calculate_age,
    calculate_age_from_dob,
    curate_shows,
    handle_talent_files,
    next_step
    )

from werkzeug.utils import secure_filename
import os
from datetime import date, datetime, timedelta

cultural_bp = Blueprint("cultural_bp", __name__)

@cultural_bp.route("/program/cultural_fire")
@login_required
def cultural_fire_home():
    user_id = current_user.id
    subj_id = request.args.get("subject_id")

    enrollment = UserEnrollment.query.filter_by(user_id=user_id, subject_id=subj_id).first()
    return render_template("auth/bridge_dashboard.html")
    #return render_template("bridge/index.html", dashboards=dashboards)

@cultural_bp.route("/program/cultural_fire/about", methods=["GET"])
def cultural_fire_about():
    slug = "cultural_fire"

    subj = AuthSubject.query.filter_by(slug=slug).first()
    if not subj:
        abort(404)

    # ✅ unified baton
    session["baton"] = {
        "subject_slug": subj.slug,
        "subject_id": subj.id
    }

    return render_template("program_culturefire/about.html")

@cultural_bp.route("/program/cultural_fire/price", methods=["GET"])
def cultural_fire_price():
    return render_template("program_culturefire/price.html")

@cultural_bp.route("/submissions")
def submissions():
    return render_template("cultural_fire/submissions.html")

@cultural_bp.route("/events")
def events():
    return render_template("cultural_fire/events.html")

@cultural_bp.route("/tickets")
def tickets():
    return render_template("cultural_fire/tickets.html")

@cultural_bp.route("/voting")
def voting():
    return render_template("cultural_fire/voting.html")

@cultural_bp.route("/volunteers")
def volunteers():
    return render_template("cultural_fire/volunteers.html")

@cultural_bp.route("/outreach")
def outreach():
    return render_template("cultural_fire/outreach.html")

@cultural_bp.route("/analytics")
def analytics():
    return render_template("cultural_fire/analytics.html")

@cultural_bp.route("/program/cultural_fire/volunteer/<int:subject_id>")
@login_required
def cultural_fire_volunteer_dashboard(subject_id):
    return render_template("program_culturefire/volunteer_dashboard.html")

@cultural_bp.route("/program/cultural_fire/start/<int:subject_id>")
@login_required
def start_program(subject_id):
    subj = AuthSubject.query.get_or_404(subject_id)
    # For now just render a placeholder template
    return render_template("program_culturefire/start_program.html", subject=subj)

@cultural_bp.route("/program/cultural_fire/progress/<int:subject_id>")
@login_required
def view_progress(subject_id):
    subj = AuthSubject.query.get_or_404(subject_id)
    # For now, just render a placeholder template
    return render_template("program_culturefire/view_progress.html", subject=subj)

@cultural_bp.route("/program/cultural_fire/resources/<int:subject_id>")
@login_required
def resources(subject_id):
    subj = AuthSubject.query.get_or_404(subject_id)
    return render_template("program_culturefire/resources.html", subject=subj)

@cultural_bp.route("/program/cultural_fire/participant/submit", methods=["GET", "POST"])
def submit_talent():
    if not current_user.is_authenticated:
        return redirect(url_for("auth_bp.login"))

    baton = session.get("baton")
    if not baton:
        abort(400)

    subject_id = baton["subject_id"]

    if request.method == "POST":
        category = request.form.get("category")
        media_url = request.form.get("media_url")

        submission = CfiTalentSubmission(
            user_id=current_user.id,
            subject_id=subject_id,
            category=category,
            media_url=media_url,
            status="pending"
        )

        db.session.add(submission)
        db.session.commit()

        return redirect(url_for("cultural_bp.participant_solo_dashboard"))

    return render_template("program_culturefire/submit.html")

@cultural_bp.route("/biodata/<int:step>", methods=["GET", "POST"])
@login_required
def biodata(step):
    user_id = current_user.id
    record = CfiBiodata.query.filter_by(user_id=user_id).first()

    # Pick the right form for the step
    if step == 1:
        form = EnrollmentStep1Form(obj=record)
    elif step == 2:
        form = EnrollmentStep2Form(obj=record)
    elif step == 3:
        form = EnrollmentStep3Form(obj=record)
    else:
        abort(404)

    if form.validate_on_submit():
        if record:
            form.populate_obj(record)
        else:
            record = CfiBiodata(user_id=user_id)
            form.populate_obj(record)
            db.session.add(record)
        db.session.commit()

        # Decide next step
        next = next_step(record)
        if next:
            return redirect(url_for("cultural_bp.biodata", step=next))
        else:
            enrollment = UserEnrollment.query.filter_by(user_id=user_id, status="started").first()
            return redirect(url_for("cultural_bp.participant_solo_dashboard", subject_id=enrollment.id))

    return render_template(f"program_culturefire/enrollment_step{step}.html", form=form)

@cultural_bp.route("/router")
@login_required
def cultural_fire_router():
    user_id = current_user.id

    # --- Resolve enrollment (single source of truth) ---
    cf_subject = AuthSubject.query.filter_by(slug='cultural_fire').first()
    if cf_subject:
        enrollment = UserEnrollment.query.filter_by(user_id=user_id, subject_id=cf_subject.id).first()
    else:
        enrollment = None
        
    if not enrollment:
        # No enrollment → redirect to registration gateway
        return redirect(url_for("registration_bp.register"))

    subject_id = enrollment.subject_id

    # --- Resolve biodata record ---
    record = CfiBiodata.query.filter_by(user_id=user_id).first()

    # Step 1: No biodata at all OR missing essentials
    if not record or not record.full_name or not record.dob or not record.phone:
        return redirect(url_for("cultural_bp.enrollment_step1", subject_id=subject_id))

    # Step 2: Missing extended details
    if not record.gender or not record.city or not record.province or not record.address_line \
       or not record.occupation or not record.highest_qualification:
        return redirect(url_for("cultural_bp.enrollment_step2", subject_id=subject_id))

    # Step 3: Pledge and role check
    if record.pledge_agreed is not True or not record.role:
        flash("You must accept the pledge to continue registration.")
        return redirect(url_for("cultural_bp.enrollment_step3", subject_id=subject_id))

    # --- Ensure enrollment is persisted ---
    if enrollment not in db.session:
        db.session.add(enrollment)
    db.session.commit()

    # --- Role-based dashboard routing ---
    dashboards = {
        "participant": "cultural_bp.talent_dashboard",
        "parent": "cultural_bp.parent_dashboard",
        "sponsor": "cultural_bp.sponsor_dashboard",
        "supporter": "cultural_bp.supporter_dashboard",
        "admin": "cultural_bp.admin_dashboard"
    }

    target_dashboard = dashboards.get(record.role, "cultural_bp.talent_dashboard")

    return redirect(url_for(target_dashboard, enrollment_id=enrollment.id))

@cultural_bp.route("/program/cultural_fire/enrollment/<int:subject_id>/step2", methods=["GET", "POST"])
@login_required
def enrollment_step2(subject_id):
    subj = AuthSubject.query.get_or_404(subject_id)
    user_id = current_user.id

    # ✅ Always fetch the record first
    record = CfiBiodata.query.filter_by(user_id=user_id).first()

    form = EnrollmentStep2Form(obj=record)

    if form.validate_on_submit():
        if not record:
            # Normally this should never happen, because Step 1 creates the record
            record = CfiBiodata(user_id=user_id)
            db.session.add(record)

        # ✅ Save Step 2 values
        record.gender = form.gender.data
        record.city = form.city.data
        record.province = form.province.data
        record.address_line = form.address_line.data
        record.occupation = form.occupation.data
        record.highest_qualification = form.highest_qualification.data

        db.session.commit()
        return redirect(url_for("cultural_bp.enrollment_step3", subject_id=subject_id))

    return render_template("program_culturefire/enrollment_step2.html", subject=subj, form=form)

@cultural_bp.route("/program/cultural_fire/enrollment/<int:subject_id>/step1", methods=["GET", "POST"])
@login_required
def enrollment_step1(subject_id):
    form = EnrollmentStep1Form()
    subj = AuthSubject.query.get_or_404(subject_id)
    user_id = current_user.id

    record = CfiBiodata.query.filter_by(user_id=user_id).first()

    if form.validate_on_submit():
        if not record:
            record = CfiBiodata(user_id=user_id)
            db.session.add(record)

        # ✅ Save Step 1 values
        record.full_name = form.full_name.data
        record.dob = form.dob.data
        record.id_number = form.id_number.data
        record.phone = form.phone.data

        db.session.commit()
        return redirect(url_for("cultural_bp.enrollment_step2", subject_id=subject_id))

    return render_template("program_culturefire/enrollment_step1.html", subject=subj, form=form)

@cultural_bp.route("/program/cultural_fire/enrollment/<int:subject_id>/step3", methods=["GET", "POST"])
@login_required
def enrollment_step3(subject_id):
    subj = AuthSubject.query.get_or_404(subject_id)
    user_id = current_user.id
    record = CfiBiodata.query.filter_by(user_id=user_id).first()

    form = EnrollmentStep3Form(obj=record)

    if form.validate_on_submit():
        if not record:
            # Normally this should never happen, because Step 1 creates the record.
            record = CfiBiodata(user_id=user_id)
            db.session.add(record)

        # ✅ Save Step 3 values

        record.role = form.role.data
        record.pledge_agreed = True   # Boolean, not int
        record.pledge_date = datetime.utcnow()

        # Flip enrollment status
        # ✅ Ensure enrollment exists and flip status
        enrollment = UserEnrollment.query.filter_by(user_id=user_id).first()
        if not enrollment:
            enrollment = UserEnrollment(biodata_id=record.id, user_id=user_id, status="started")
            
            db.session.add(enrollment)
        else:
            enrollment.status = "started"

        db.session.commit()
        print("Saved role:", record.role)

        # After Step 3, always return to router
        return redirect(url_for("cultural_bp.cultural_fire_router"))

    return render_template("program_culturefire/enrollment_step3.html", form=form, subject=subj)

# --- new design dashboard ----
# New Talent form
@cultural_bp.route("/talent/new/<int:enrollment_id>", methods=["GET", "POST"])
@login_required
def talent_new(enrollment_id):
    enrollment = UserEnrollment.query.get_or_404(enrollment_id)
    form = TalentSubmissionForm()

    if request.method == "POST":
        submission_type = request.form.get("submission_type")
        category_item_id = request.form.get("category_id", type=int)

        custom_talent = request.form.get("custom_talent")
        talent_name = request.form.get("talent_name")
        collaborators = request.form.getlist("collaborators")
        files = request.files.getlist("talent_files")

        category_obj = CfiTalentCategoryItem.query.get(category_item_id)
        is_other = category_obj and "other" in category_obj.name.lower()

        # Find or create active show for this category
        active_show = CfiShow.query.filter_by(
            category_item_id=category_item_id,
            status="active"
        ).first()

        if not active_show:
            shows_count = CfiShow.query.filter_by(category_item_id=category_item_id).count()
            active_show = CfiShow(
                title=f"{category_obj.name} Show {shows_count + 1}",
                description=f"{category_obj.name} Showcase",
                start_date=date.today(),
                location="TBD",
                status="active",
                category_item_id=category_item_id
            )
            db.session.add(active_show)
            db.session.flush()

        submission = CfiTalentSubmission(
            user_id=enrollment.user_id,
            subject_id=enrollment.subject_id,
            user_enrollment_id=enrollment.id,  # ✅ THIS IS THE FIX
            category_item_id=category_item_id,
            show_id=active_show.id,
            custom_talent=custom_talent if is_other else None,
            talent_name=talent_name
        )

        db.session.add(submission)
        db.session.flush()  # 🔥 ensures submission.id exists

        # collaborators
        if submission_type == "group":
            for c_id in collaborators:
                collaborator = UserEnrollment.query.get(c_id)
                if collaborator:
                    submission.participants.append(
                        CfiSubmissionParticipant(user_id=collaborator.user_id)
                    )

        # 🔥 SAVE FILES (now ID is guaranteed)
        saved_files = []

        for file in files:
            if file and file.filename:
                filename = build_filename(talent_name, file.filename, submission.id)

                file.save(os.path.join(os.path.join(current_app.root_path, "static", "uploads"), filename))

                saved_files.append(filename)
                submission.files.append(CfiTalentFile(filename=filename))

        if saved_files:
            submission.media_url = url_for('static', filename=f'uploads/{saved_files[0]}')

        db.session.commit()

        flash("New talent submitted successfully!", "success")
        return redirect(url_for(
            "cultural_bp.talent_dashboard",
            enrollment_id=enrollment.id
        ))

    categories = CfiTalentCategoryItem.query.all()
    groups = CfiGroup.query.all()
    enrollments = UserEnrollment.query.all()

    # Get optional category_id from query string to preselect in the form
    selected_category_id = request.args.get("category_id", type=int)

    return render_template(
        "program_culturefire/talent_new.html",
        enrollment=enrollment,
        groups=groups,
        enrollments=enrollments,
        categories=categories,
        selected_category_id=selected_category_id,
        form=form
    )

# Showcase page
@cultural_bp.route("/talent/showcase")
@login_required
def talent_showcase():
    return render_template("program_culturefire/talent_showcase.html")

@cultural_bp.route("/talent/<int:submission_id>/group/create/<int:enrollment_id>", methods=["GET", "POST"])
@login_required
def talent_group_create(submission_id, enrollment_id):
    # Fetch the talent submission and enrollment
    submission = CfiTalentSubmission.query.get_or_404(submission_id)
    enrollment = UserEnrollment.query.get_or_404(enrollment_id)

    # Gatekeeper: check if a group already exists for this submission
    existing_group = CfiGroup.query.filter_by(submission_id=submission.id).first()

    if existing_group:
        # Instead of showing the create form, render a table view with Edit action
        return render_template(
            "program_culturefire/talent_group_dashboard.html",
            group=existing_group,
            submission=submission,
            enrollment=enrollment
        )

    # Otherwise continue with create form logic
    cfi_subject = AuthSubject.query.filter_by(slug="cultural_fire").first_or_404()
    all_users = UserEnrollment.query.filter_by(subject_id=cfi_subject.id).all()

    form = NewGroupForm()
    form.member_ids.choices = [(u.id, f"{u.user_id} – {u.status}") for u in all_users]

    if form.validate_on_submit():
        new_group = CfiGroup(
            name=form.group_name.data,
            leader_id=enrollment.id,
            submission_id=submission.id
        )
        db.session.add(new_group)
        db.session.flush()

        member_ids = set(int(mid) for mid in form.member_ids.data if mid)
        member_ids.add(enrollment.id)

        for mid in member_ids:
            db.session.add(
                CfiGroupMember(
                    group_id=new_group.id,
                    enrollment_id=mid,
                    submission_id=submission.id
                )
            )

        db.session.commit()
        flash("Group created successfully!", "success")
        return redirect(url_for("cultural_bp.talent_dashboard", enrollment_id=enrollment.id))

    return render_template(
        "program_culturefire/talent_group_dashboard.html",
        form=form,
        submission=submission,
        enrollment=enrollment,
        group=None
    )

'''
# 🔹 Talent Dashboard
@cultural_bp.route("/program/cultural_fire/talent/dashboard/<int:enrollment_id>", methods=["GET"])
@login_required
def talent_dashboard(enrollment_id):
    enrollment = UserEnrollment.query.get_or_404(enrollment_id)

    show_all = request.args.get("all", "false").lower() == "true"

    talents_all = (
        CfiTalentSubmission.query
        .filter_by(user_id=enrollment.user_id, subject_id=enrollment.subject_id)
        .order_by(CfiTalentSubmission.id.desc())
        .all()
    )

    talents = talents_all if show_all else talents_all[:3]
    categories = CfiTalentCategoryItem.query.all()
    return render_template(
        "program_culturefire/talent_dashboard.html",
        enrollment=enrollment,
        enrollment_id=enrollment_id,
        cfi_talent_category_items=categories,
        talents=talents,
        total_count=len(talents_all),
        showing_count=len(talents),
        show_all=show_all
    )
'''

@cultural_bp.route("/program/cultural_fire/talent/details/<int:submission_id>", methods=["GET", "POST"])
@login_required
def talent_details(submission_id):
    talent = CfiTalentSubmission.query.get_or_404(submission_id)

    # Load context, sponsors, supporters
    context_options = CfiTalentContext.query.all()
    sponsors = (
        User.query
        .join(UserEnrollment, User.id == UserEnrollment.user_id)
        .join(CfiBiodata, UserEnrollment.biodata_id == CfiBiodata.id)
        .filter(CfiBiodata.role == "sponsor")
        .all()
    )
    supporters = (
        User.query
        .join(UserEnrollment, User.id == UserEnrollment.user_id)
        .join(CfiBiodata, UserEnrollment.biodata_id == CfiBiodata.id)
        .filter(CfiBiodata.role == "supporter")
        .all()
    )

    # Pre-populate form with existing talent values
    form = TalentDetailsForm(obj=talent)

    # Populate choices
    form.context.choices = [(c.id, c.name) for c in context_options]
    form.sponsor_id.choices = [(s.id, s.name) for s in sponsors]
    form.supporter_id.choices = [(s.id, s.name) for s in supporters]

    if form.validate_on_submit():
        talent.context_id = form.context.data
        talent.sponsor_id = form.sponsor_id.data
        talent.supporter_id = form.supporter_id.data
        db.session.commit()
        flash("Talent details updated successfully.", "success")

        # ✅ Always redirect back to Talent Dashboard with enrollment_id
        enrollment_id = talent.user_enrollment_id

        if not enrollment_id:
            abort(400, "Missing enrollment_id for redirect")

        return redirect(url_for("cultural_bp.talent_dashboard", enrollment_id=enrollment_id))

    return render_template(
        "program_culturefire/talent_details.html",
        talent=talent,
        form=form,
        context_options=context_options,
        sponsors=sponsors,
        supporters=supporters
    )

@cultural_bp.route("/talent/edit/<int:submission_id>", methods=["GET", "POST"])
@login_required
def talent_edit(submission_id):
    talent = CfiTalentSubmission.query.get_or_404(submission_id)
    enrollment = UserEnrollment.query.filter_by(user_id=talent.user_id).first_or_404()

    form = TalentForm(obj=talent)

    form.category_item_id.choices = [
        (c.id, c.name) for c in CfiTalentCategoryItem.query.all()
    ]

    if form.validate_on_submit():
        # Update fields
        talent.talent_name = form.talent_name.data
        talent.custom_talent = form.custom_talent.data
        talent.category_item_id = form.category_item_id.data

        # 🔥 DELETE old files (DB + disk)
        for f in talent.files:
            try:
                os.remove(os.path.join(os.path.join(current_app.root_path, "static", "uploads"), f.filename))
            except Exception:
                pass

        CfiTalentFile.query.filter_by(submission_id=talent.id).delete()
        talent.files.clear()

        # 🔥 SAVE new files
        #files = request.files.getlist("talent_files")
        files = form.talent_files.data

        saved_files = []

        for file in files:
            if file and file.filename:
                filename = build_filename(talent.talent_name, file.filename, talent.id)

                file.save(os.path.join(os.path.join(current_app.root_path, "static", "uploads"), filename))

                saved_files.append(filename)
                talent.files.append(CfiTalentFile(filename=filename))

        if saved_files:
            talent.media_url = url_for('static', filename=f'uploads/{saved_files[0]}')

        db.session.commit()

        flash("Talent updated successfully!", "success")
        return redirect(url_for(
            "cultural_bp.talent_dashboard",
            enrollment_id=enrollment.id
        ))

    return render_template(
        "program_culturefire/talent_edit.html",
        talent=talent,
        form=form,
        enrollment=enrollment
    )

@cultural_bp.route("/uploads/<path:filename>")
def uploaded_file(filename):
    return send_from_directory(os.path.join(current_app.root_path, "static", "uploads"), filename)

@cultural_bp.route("/talent/group/edit/<int:submission_id>", methods=["GET", "POST"])
@login_required
def talent_group_edit(submission_id):
    # Load the talent submission
    talent = CfiTalentSubmission.query.get_or_404(submission_id)

    # Find the group tied to this submission
    group = CfiGroup.query.filter_by(submission_id=submission_id).first()

    # Always resolve an enrollment (leader if group exists, fallback otherwise)
    if group:
        enrollment = UserEnrollment.query.get_or_404(group.leader_id)
    else:
        enrollment = UserEnrollment.query.filter_by(
            user_id=current_user.id,
            subject_id=talent.subject_id
        ).first()

    # Build the form
    form = UpdateGroupForm()
    form.member_ids.choices = [
        (u.id, u.user.name)
        for u in UserEnrollment.query.filter_by(subject_id=talent.subject_id).all()
    ]

    if form.validate_on_submit():
        if group:
            # Update existing group
            group.name = form.group_name.data
            CfiGroupMember.query.filter_by(group_id=group.id, submission_id=submission_id).delete()
            for mid in form.member_ids.data:
                db.session.add(
                    CfiGroupMember(group_id=group.id, enrollment_id=mid, submission_id=submission_id)
                )

            talent.group_id = group.id
            db.session.add(talent)

        else:
            # Create new group
            group = CfiGroup(
                name=form.group_name.data,
                leader_id=enrollment.id,
                submission_id=submission_id
            )
            db.session.add(group)
            db.session.flush()  # ensures group.id is available

            talent.group_id = group.id
            db.session.add(talent)

            for mid in form.member_ids.data:
                db.session.add(
                    CfiGroupMember(group_id=group.id, enrollment_id=mid, submission_id=submission_id)
                )

        db.session.commit()
        flash("Group saved successfully!", "success")

        # 🔹 FIX: redirect with group_id, not submission_id
        return redirect(url_for("cultural_bp.talent_group_dashboard", group_id=group.id))

    # Pre-fill form if group exists
    if group:
        form.group_name.data = group.name
        form.member_ids.data = [gm.enrollment_id for gm in group.group_members]

    return render_template(
        "program_culturefire/talent_group_edit.html",
        group=group,
        talent=talent,
        enrollment=enrollment,
        form=form
    )

@cultural_bp.route("/talent/group/dashboard/<int:group_id>")
@login_required
def talent_group_dashboard(group_id):
    group = CfiGroup.query.get_or_404(group_id)
    submission = group.submission
    enrollment = group.leader  # or whichever enrollment you want to show

    return render_template(
        "program_culturefire/talent_group_dashboard.html",
        group=group,
        submission=submission,
        enrollment=enrollment
    )

# --- parent---

@cultural_bp.route("/parent/dashboard/<int:enrollment_id>", methods=["GET"], endpoint="parent_dashboard")
@login_required
def parent_dashboard(enrollment_id):
    cfi_subject = AuthSubject.query.filter_by(slug="cultural_fire").first_or_404()

    # Get the parent’s enrollment record
    enrollment = UserEnrollment.query.get_or_404(enrollment_id)

    links = CfiParent.query.filter_by(parent_id=current_user.id).all()
    children = []

    for link in links:
        child_enrollment = (
            UserEnrollment.query
            .filter_by(user_id=link.child_id, subject_id=cfi_subject.id)
            .first()
        )
        if not child_enrollment:
            continue

        biodata = CfiBiodata.query.filter_by(user_id=link.child_id).first()

        talents = (
            CfiTalentSubmission.query
            .filter_by(user_id=link.child_id)
            .order_by(CfiTalentSubmission.id.asc())
            .all()
        )

        groups = (
            CfiGroup.query
            .join(CfiGroupMember, CfiGroupMember.group_id == CfiGroup.id)
            .filter(CfiGroupMember.enrollment.has(user_id=link.child_id))
            .all()
        )

        group_data = []
        for g in groups:
            group_data.append({
                "id": g.id,
                "name": g.name,
                "members": g.members,
                "submission_id": g.submission_id
            })

        children.append({
            "user": child_enrollment.user,
            "biodata": biodata,
            "status": child_enrollment.status,
            "subject": child_enrollment.subject,
            "talents": talents,
            "groups": groups,
            "permission_granted": link.permission_granted
        })

    form = PermissionForm()
    return render_template(
        "program_culturefire/parent_dashboard.html", 
        children=children, 
        form=form,
        enrollment=enrollment,   # pass the parent’s enrollment object
        calculate_age_from_dob=calculate_age_from_dob
    )

@cultural_bp.route("/parent/add", methods=["GET", "POST"], endpoint="parent_add_participant")
def parent_add_participant():
    form = ParentAddParticipantForm()

    # Get the cultural_fire subject
    cfi_subject = AuthSubject.query.filter_by(slug="cultural_fire").first_or_404()
    all_users = UserEnrollment.query.filter_by(subject_id=cfi_subject.id).all()

    # Populate dropdown choices BEFORE validation
    form.child_id.choices = [
        (enrollment.user.id, enrollment.user.name)
        for enrollment in all_users
    ]

    if form.validate_on_submit():
        child_id = form.child_id.data
        relationship = form.relationship.data
        consent = form.consent.data

        new_link = CfiParent(
            parent_id=current_user.id,
            child_id=child_id,
            relationship=relationship,
            consent=consent
        )
        db.session.add(new_link)
        db.session.commit()

        flash("Participant linked successfully!", "success")
        return redirect(url_for("cultural_bp.parent_dashboard"))

    return render_template(
        "program_culturefire/parent_add_participant.html",
        cfi_users=all_users,
        form=form
    )

@cultural_bp.route("/parent/permission", methods=["POST"])
@login_required
def toggle_permission():
    child_id = request.form.get("child_id")
    item_id = request.form.get("item_id")
    item_type = request.form.get("item_type")
    action = request.form.get("action")

    print(f"[DEBUG] toggle_permission: child_id={child_id}, item_id={item_id}, item_type={item_type}, action={action}")

    if item_type == "talent":
        talent = CfiTalentSubmission.query.get_or_404(item_id)
        if action == "grant":
            talent.permission_granted = True
        elif action == "revoke":
            talent.permission_granted = False

        db.session.commit()
        flash(f"Permission {action}ed for {talent.talent_name}", "success")

    return redirect(url_for("cultural_bp.parent_dashboard"))

@cultural_bp.route("/parent/update_biodata/<int:child_id>", methods=["GET", "POST"])
@login_required
def parent_update_biodata(child_id):
    biodata = CfiBiodata.query.filter_by(user_id=child_id).first_or_404()
    form = BiodataForm(obj=biodata)

    if form.validate_on_submit():
        form.populate_obj(biodata)
        db.session.commit()
        flash("Bio data updated successfully.", "success")
        return redirect(url_for("cultural_bp.parent_dashboard"))

    return render_template("program_culturefire/parent_update_biodata.html", form=form, child_id=child_id)

# --- sponsors---

@cultural_bp.route("/sponsor/dashboard")
@login_required
def sponsor_dashboard():
    enrollment = UserEnrollment.query.filter_by(user_id=current_user.id).first()

    sponsorships = (
        db.session.query(CfiSponsorship)
        .outerjoin(UserEnrollment, CfiSponsorship.participant_id == UserEnrollment.id)
        .outerjoin(CfiBiodata, UserEnrollment.biodata_id == CfiBiodata.id)
        .outerjoin(CfiShow, CfiSponsorship.show_id == CfiShow.id)
        .filter(CfiSponsorship.user_id == current_user.id)
        .order_by(CfiSponsorship.created_at.desc())
        .all()
    )

    return render_template(
        "program_culturefire/sponsor_dashboard.html",
        sponsorships=sponsorships,
        enrollment_id=enrollment.id if enrollment else None
    )

# routes.py
@cultural_bp.route("/sponsor/create/<int:enrollment_id>", methods=["GET", "POST"])
@login_required
def sponsor_create(enrollment_id):
    form = SponsorForm()

    # Populate participant dropdown
    users = (
        UserEnrollment.query
        .join(CfiBiodata, UserEnrollment.biodata_id == CfiBiodata.id)
        .all()
    )
    form.participant_id.choices = [(0, "-- None --")] + [
        (u.id, f"{u.biodata.full_name} ({u.biodata.role})")
        for u in users
    ]

    # Sponsorship items
    form.item_id.choices = [
        (i.id, f"{i.name} (R{i.amount})") for i in CfiSponsorItem.query.all()
    ]

    # Shows
    form.show_id.choices = [(0, "-- None --")] + [
        (s.id, s.title) for s in CfiShow.query.all()
    ]

    # Default to "-- None --" on GET
    if request.method == "GET":
        form.participant_id.data = 0
        form.show_id.data = 0

    if form.validate_on_submit():
        print("Sponsor form POST data:", request.form.to_dict())

        # Require at least one of participant or show
        if (not form.participant_id.data or form.participant_id.data == 0) and \
           (not form.show_id.data or form.show_id.data == 0):
            flash("You must select either a participant or a show.", "error")
            return render_template("program_culturefire/sponsor_create.html",
                                   form=form, enrollment_id=enrollment_id)

        # Require sponsorship item
        if not form.item_id.data or form.item_id.data == 0:
            flash("You must select a sponsorship item.", "error")
            return render_template("program_culturefire/sponsor_create.html",
                                   form=form, enrollment_id=enrollment_id)

        participant_id = None if form.participant_id.data == 0 else form.participant_id.data

        talent_submission = (
            CfiTalentSubmission.query
            .filter_by(user_enrollment_id=participant_id)
            .first()
            if participant_id else None
        )

        item = CfiSponsorItem.query.get(form.item_id.data)

        sponsorship = CfiSponsorship(
            user_id=current_user.id,
            participant_id=participant_id,
            sponsor_item_id=form.item_id.data,
            show_id=form.show_id.data if form.show_id.data != 0 else None,
            talent_submission_id=talent_submission.id if talent_submission else None,
            amount=item.amount if item else 0
        )

        db.session.add(sponsorship)
        db.session.commit()

        flash("Sponsorship added successfully!", "success")
        return redirect(url_for("cultural_bp.sponsor_dashboard"))

    # GET or failed validation
    return render_template(
        "program_culturefire/sponsor_create.html",
        form=form,
        enrollment_id=enrollment_id
    )

@cultural_bp.route("/sponsor/edit/<int:id>", methods=["GET", "POST"])
@login_required
def sponsor_edit(id):
    sponsor = CfiSponsorship.query.get_or_404(id)
    form = SponsorForm(obj=sponsor)

    # Populate participant dropdown
    users = (
        UserEnrollment.query
        .join(CfiBiodata, UserEnrollment.biodata_id == CfiBiodata.id)
        .all()
    )
    form.participant_id.choices = [(0, "-- None --")] + [
        (u.id, f"{u.biodata.full_name} ({u.biodata.role})")
        for u in users
    ]

    # Sponsorship items
    form.item_id.choices = [
        (i.id, f"{i.name} (R{i.amount})") for i in CfiSponsorItem.query.all()
    ]

    # Shows
    form.show_id.choices = [(0, "-- None --")] + [
        (s.id, s.title) for s in CfiShow.query.all()
    ]

    if form.validate_on_submit():
        sponsor.participant_id = form.participant_id.data if form.participant_id.data != 0 else None
        sponsor.show_id = form.show_id.data if form.show_id.data != 0 else None
        sponsor.sponsor_item_id = form.item_id.data

        item = CfiSponsorItem.query.get(form.item_id.data)
        sponsor.amount = item.amount if item else sponsor.amount

        db.session.commit()
        flash("Sponsorship updated successfully!", "success")
        return redirect(url_for("cultural_bp.sponsor_dashboard"))

    return render_template(
        "program_culturefire/sponsor_edit.html",
        form=form,
        sponsor=sponsor
    )

@cultural_bp.route("/sponsor/checkout/<int:id>", methods=["GET"])
@login_required
def sponsor_checkout(id):
    sponsor = CfiSponsorship.query.get_or_404(id)

    # Build Yoco checkout URL (example)
    yoco_url = f"https://pay.yoco.com/{sponsor.id}?amount={sponsor.amount:.2f}"
    return redirect(yoco_url)

# --- supporter---

@cultural_bp.route("/supporter/dashboard/<int:enrollment_id>")
@login_required
def supporter_dashboard(enrollment_id):
    # Get the enrollment object
    enrollment = UserEnrollment.query.get_or_404(enrollment_id)

    supporters = (
        db.session.query(CfiSupporter, UserEnrollment, CfiBiodata, CfiShow)
        .outerjoin(UserEnrollment, CfiSupporter.participant_id == UserEnrollment.id)
        .outerjoin(CfiBiodata, UserEnrollment.biodata_id == CfiBiodata.id)
        .outerjoin(CfiShow, CfiSupporter.show_id == CfiShow.id)  # if you have a show_id field
        .filter(CfiSupporter.user_id == current_user.id)
        .all()
    )

    form = SupporterForm()
    return render_template(
        "program_culturefire/supporter_dashboard.html",
        supporters=supporters,
        enrollment=enrollment,   # pass the object, not just the id
        enrollment_id=enrollment.id,   # add this line
        form=form
    )

@cultural_bp.route("/supporter/create/<int:enrollment_id>", methods=["GET", "POST"])
@login_required
def supporter_create(enrollment_id):
    form = SupporterForm()

    # Query all users with biodata once
    users = (
        UserEnrollment.query
        .join(CfiBiodata, UserEnrollment.biodata_id == CfiBiodata.id)
        .all()
    )

    # Participant dropdown (with None option)
    form.participant_id.choices = [(0, "-- None --")] + [
        (u.id, f"{u.biodata.full_name} ({u.biodata.role})")
        for u in users
    ]

    # Referee dropdown: same list, but exclude current supporter
    form.referee_id.choices = [
        (u.id, f"{u.biodata.full_name} ({u.biodata.role})")
        for u in users
        if u.user_id != current_user.id
    ]

    if form.validate_on_submit():
        participant_id = None if form.participant_id.data == 0 else form.participant_id.data

        if not form.referee_id.data:
            flash("A referee must be selected to authenticate this supporter.", "error")
            return render_template("program_culturefire/supporter_create.html", form=form)

        supporter = CfiSupporter(
            user_id=current_user.id,
            participant_id=participant_id,
            referee_id=form.referee_id.data,
            #amount=form.amount.data,
            #note=form.note.data,
            duration_months=form.duration_months.data,
            supporter_type=form.supporter_type.data,
            #availability=form.availability.data,
            #stipend_required=form.stipend_required.data
        )
        db.session.add(supporter)
        db.session.commit()
        return redirect(url_for("cultural_bp.supporter_dashboard", enrollment_id=enrollment_id))

    return render_template(
        "program_culturefire/supporter_create.html",
        form=form,
        enrollment_id=enrollment_id
    )

@cultural_bp.route("/supporter/showcase/<int:enrollment_id>")
@login_required
def supporter_showcase(enrollment_id):
    supporters = CfiSupporter.query.filter_by(enrollment_id=enrollment_id).all()
    return render_template(
        "program_culturefire/supporter_showcase.html",
        supporters=supporters,
        enrollment_id=enrollment_id
    )

@cultural_bp.route("/supporter/edit/<int:id>", methods=["GET", "POST"])
@login_required
def supporter_edit(id):
    supporter = CfiSupporter.query.get_or_404(id)
    form = SupporterForm(obj=supporter)

    if form.validate_on_submit():
        form.populate_obj(supporter)
        db.session.commit()
        flash("Supporter updated successfully", "success")
        return redirect(url_for("cultural_bp.supporter_dashboard", id=id))

    return render_template("program_culturefire/supporter_edit.html", form=form, supporter=supporter)

@cultural_bp.route("/supporter/details/<int:id>", methods=["GET"])
@login_required
def supporter_details(id):
    supporter = CfiSupporter.query.get_or_404(id)
    return render_template(
        "program_culturefire/supporter_details.html",
        supporter=supporter
    )

@cultural_bp.route("/supporter/confirm/<int:id>", methods=["POST", "GET"])
@login_required
def supporter_confirm(id):
    supporter = CfiSupporter.query.get_or_404(id)
    supporter.confirmed = True   # assuming you have a 'confirmed' column
    db.session.commit()
    flash("Supporter confirmed successfully", "success")
    return redirect(url_for("cultural_bp.supporter_dashboard", id=id))

# --- showcase---
@cultural_bp.route("/showcase/dashboard")
@login_required
def showcase_dashboard():
    origin = request.args.get("origin")
    enrollment_id = request.args.get("enrollment_id")
    parent_id = request.args.get("parent_id")
    supporter_id = request.args.get("supporter_id")
    sponsor_id = request.args.get("sponsor_id")

    if origin:
        session['showcase_origin'] = origin
        if enrollment_id: session['showcase_enrollment_id'] = enrollment_id
        if parent_id: session['showcase_parent_id'] = parent_id
        if supporter_id: session['showcase_supporter_id'] = supporter_id
        if sponsor_id: session['showcase_sponsor_id'] = sponsor_id

    origin = session.get('showcase_origin', 'talent')
    enrollment_id = session.get('showcase_enrollment_id')
    parent_id = session.get('showcase_parent_id')
    supporter_id = session.get('showcase_supporter_id')
    sponsor_id = session.get('showcase_sponsor_id')

    # Run curate helper before displaying
    # (Disabled per user request: shows are now created instantly by category)
    submissions = CfiTalentSubmission.query.all()

    shows = CfiShow.query.all()
    pageant_segments = [s for s in CfiPageantSegment.query.all() if s.name.lower() not in ('sponsor', 'supporter')]

    # Collect related data
    groups = CfiGroup.query.all()
    sponsors = CfiSponsorship.query.all()
    supporters = CfiSupporter.query.all()
    
    judge_assignments = CfiJudgeAssignment.query.filter_by(judge_id=current_user.id).all()
    user_submissions = CfiTalentSubmission.query.filter_by(user_id=current_user.id).all()

    return render_template(
        "program_culturefire/showcase_dashboard.html",
        shows=shows,
        pageant_segments=pageant_segments,
        groups=groups,
        sponsors=sponsors,
        supporters=supporters,
        judge_assignments=judge_assignments,
        user_submissions=user_submissions,
        origin=origin,
        enrollment_id=enrollment_id,
        parent_id=parent_id,
        supporter_id=supporter_id,
        sponsor_id=sponsor_id
    )

@cultural_bp.route("/show/program/<int:show_id>")
@login_required
def show_program(show_id):
    origin = request.args.get("origin")
    enrollment_id = request.args.get("enrollment_id")
    show = CfiShow.query.get_or_404(show_id)

    if show.category_item and show.category_item.name == "Pageant":
        segment_items = (CfiSegmentItem.query
                       .filter_by(show_id=show.id)
                       .options(joinedload(CfiSegmentItem.enrollment)
                                .joinedload(UserEnrollment.biodata))
                       .all())
        
        # Group by segment_type
        submissions_by_segment = {}
        for item in segment_items:
            seg = item.segment_type.replace('_', ' ').title()
            if seg not in submissions_by_segment:
                submissions_by_segment[seg] = []
            
            # Normalize attributes so it looks like a submission
            item.user_enrollment = item.enrollment
            item.talent_name = item.title
            if item.user_enrollment and item.user_enrollment.biodata and item.user_enrollment.biodata.dob:
                item.user_enrollment.biodata.age_calc = calculate_age(item.user_enrollment.biodata.dob)
                
            submissions_by_segment[seg].append(item)
                
        # Sort the dictionary by segment order
        PAGEANT_ORDER = {
            "ramp_walk": 1,
            "intro": 2,
            "talent": 3,
            "traditional_wear": 4,
            "formal_wear": 5,
            "qna": 6,
            "q&a": 6
        }
        
        # Sort by mapping the title cased name back to its key, default to 99 if not found
        sorted_segments = {}
        for seg_key in sorted(submissions_by_segment.keys(), key=lambda k: PAGEANT_ORDER.get(k.replace(' ', '_').lower(), 99)):
            sorted_segments[seg_key] = submissions_by_segment[seg_key]
            
        submissions_by_segment = sorted_segments
            
        submissions = None # We won't use the flat list for Pageants
    else:
        submissions_by_segment = None
        submissions = (CfiTalentSubmission.query
                       .filter_by(show_id=show.id)
                       .options(joinedload(CfiTalentSubmission.user_enrollment)
                                .joinedload(UserEnrollment.biodata))
                       .all())

        for sub in submissions:
            if not hasattr(sub, 'user_enrollment'):
                sub.user_enrollment = getattr(sub, 'enrollment', None)
            if not hasattr(sub, 'talent_name'):
                sub.talent_name = getattr(sub, 'title', None)
            if not hasattr(sub, 'custom_talent'):
                sub.custom_talent = None
            if not hasattr(sub, 'group_members'):
                sub.group_members = []
            if not hasattr(sub, 'sponsors'):
                sub.sponsors = []
            if not hasattr(sub, 'supporters'):
                sub.supporters = []

            if sub.user_enrollment and sub.user_enrollment.biodata and sub.user_enrollment.biodata.dob:
                sub.user_enrollment.biodata.age_calc = calculate_age(sub.user_enrollment.biodata.dob)

    return render_template("program_culturefire/program.html",
                           show=show,
                           submissions=submissions,
                           submissions_by_segment=submissions_by_segment,
                           origin=origin,
                           enrollment_id=enrollment_id
                           )

@cultural_bp.route("/show/watch/<int:show_id>")
@login_required
def watch_show(show_id):
    show = CfiShow.query.get_or_404(show_id)

    if show.category_item and show.category_item.name == "Pageant":
        submissions = (CfiSegmentItem.query
                       .filter_by(show_id=show.id)
                       .all())
        submissions_data = [
            {
                "id": sub.id,
                "title": sub.title or "Untitled",
                "segment_type": sub.segment_type,
                "src": url_for("cultural_bp.uploaded_file", filename=sub.video_url) if not sub.video_url.startswith('uploads/') else url_for("cultural_bp.uploaded_file", filename=sub.video_url.replace('uploads/', ''))
            }
            for sub in submissions if sub.video_url
        ]
    else:
        submissions = (
            CfiTalentSubmission.query
            .filter_by(show_id=show.id)
            .all()
        )
        submissions_data = [
            {
                "id": sub.id,
                "title": sub.talent_name or sub.custom_talent or "Untitled",
                "segment_type": "all",
                "src": url_for("cultural_bp.uploaded_file", filename=file.filename)
            }
            for sub in submissions
            for file in (sub.files or [])
            if file and file.filename
        ]

    # Get available segments for the UI and sort them correctly
    PAGEANT_ORDER = {
        "ramp_walk": 1,
        "intro": 2,
        "talent": 3,
        "traditional_wear": 4,
        "formal_wear": 5,
        "qna": 6,
        "q&a": 6
    }
    
    if show.category_item and show.category_item.name == "Pageant":
        raw_segments = list(set([s["segment_type"] for s in submissions_data]))
        available_segments = sorted(raw_segments, key=lambda x: PAGEANT_ORDER.get(x.replace(' ', '_').lower(), 99))
    else:
        available_segments = []

    origin = request.args.get("origin")
    enrollment_id = request.args.get("enrollment_id")

    is_judge = CfiJudgeAssignment.query.filter_by(show_id=show.id, judge_id=current_user.id).first() is not None
    return render_template(
        "program_culturefire/watch_show.html",
        is_judge=is_judge,
        show=show,
        submissions_data=submissions_data,
        available_segments=available_segments,
        origin=origin,
        enrollment_id=enrollment_id
    )

@cultural_bp.route("/showcase/archive", methods=["GET", "POST"])
@login_required
def showcase_archive():
    cutoff_date = datetime.utcnow() - timedelta(days=30)

    # Find active shows older than 30 days
    old_shows = CfiShow.query.filter(
        CfiShow.status == "active",
        CfiShow.created_at < cutoff_date
    ).all()

    for show in old_shows:
        show.status = "archived"
        show.archived_at = datetime.utcnow()

    db.session.commit()

    flash(f"{len(old_shows)} shows archived successfully.", "success")
    return redirect(url_for("cultural_bp.showcase_dashboard"))


@cultural_bp.route("/showcase/live/results")
@login_required
def live_results_list():
    origin = request.args.get('origin', 'talent')
    enrollment_id = request.args.get('enrollment_id', type=int)
    shows = CfiShow.query.all()
    pageant_shows = [s for s in shows if s.category_item and s.category_item.name == "Pageant"]
    return render_template("program_culturefire/live_results_list.html", shows=pageant_shows, origin=origin, enrollment_id=enrollment_id)

@cultural_bp.route("/showcase/live/upcoming")
@login_required
def live_shows_static():
    origin = request.args.get('origin', 'talent')
    enrollment_id = request.args.get('enrollment_id', type=int)
    return render_template("program_culturefire/live_shows_static.html", origin=origin, enrollment_id=enrollment_id)

@cultural_bp.route("/talent/select/<int:enrollment_id>")
@login_required
def talent_select(enrollment_id):
    enrollment = UserEnrollment.query.get_or_404(enrollment_id)
    category_id = request.args.get("category_id", type=int)

    if not category_id:
        flash("Please select a category.", "warning")
        return redirect(url_for("cultural_bp.talent_dashboard", enrollment_id=enrollment_id))

    category = CfiTalentCategoryItem.query.get_or_404(category_id)

    # Dispatch into guided flow
    if category.name.lower() == "pageant":
        return redirect(url_for(
            "cultural_bp.pageant_dashboard",
            enrollment_id=enrollment.id,
            category_id=category.id
        ))
    else:
        return redirect(url_for(
            "cultural_bp.talent_new",
            enrollment_id=enrollment.id,
            category_id=category.id
        ))

'''
@cultural_bp.route("/talent/pageant/<int:enrollment_id>/<int:category_id>", methods=["GET"])
@login_required
def pageant_flow(enrollment_id, category_id):
    enrollment = UserEnrollment.query.get_or_404(enrollment_id)
    category = CfiTalentCategoryItem.query.get_or_404(category_id)
    
    # Find existing shows for this category
    shows = CfiShow.query.filter_by(
        category_item_id=category.id
    ).order_by(CfiShow.id.asc()).all()

    if not shows:
        # Create Show 1 if none exist
        show = CfiShow(
            title=f"{category.name} Show 1",
            description=f"First {category.name} showcase",
            start_date=date.today(),
            location="TBD",
            status="active",
            category_item_id=category.id
        )
        db.session.add(show)
        db.session.commit()
    else:
        # Find first incomplete show
        show = next((s for s in shows if s.status != "completed"), None)
        if not show:
            # All completed → create next show
            next_number = len(shows) + 1
            show = CfiShow(
                title=f"{category.name} Show {next_number}",
                description=f"{category.name} showcase {next_number}",
                start_date=date.today(),
                location="TBD",
                status="active",
                category_item_id=category.id
            )
            db.session.add(show)
            db.session.commit()

    segments = [s for s in CfiPageantSegment.query.all() if s.name.lower() not in ('sponsor', 'supporter')]
    submissions = CfiTalentSubmission.query.filter_by(
        user_enrollment_id=enrollment.id,
        show_id=show.id
    ).all()

    # ✅ Create the form here too
    form = TalentForm()
    form.segment_id.choices = [(seg.id, seg.name) for seg in segments]

    return render_template(
        "program_culturefire/pageant_dashboard.html",
        enrollment=enrollment,
        category=category,
        show=show,
        segments=segments,
        submissions=submissions,
        form=form   # ✅ pass it in
    )


@cultural_bp.route("/talent/pageant/<int:enrollment_id>/<int:category_id>/<segment>/edit", methods=["GET", "POST"])
@login_required
def segment_edit(enrollment_id, category_id, segment):
    enrollment = UserEnrollment.query.get_or_404(enrollment_id)
    category = CfiTalentCategoryItem.query.get_or_404(category_id)
    show = CfiShow.query.filter_by(category_item_id=category.id, status="active").first_or_404()

    submission = CfiTalentSubmission.query.filter_by(
        user_enrollment_id=enrollment.id,
        category_item_id=category.id,
        show_id=show.id,
        segment_type=segment
    ).first_or_404()

    if request.method == "POST":
        file = request.files.get("video")
        if file:
            filename = secure_filename(file.filename)
            filepath = os.path.join(os.path.join(current_app.root_path, "static", "uploads"), filename)
            file.save(filepath)
            submission.video_url = filepath
            db.session.commit()
            flash(f"{segment.replace('_',' ').title()} updated successfully.", "success")
            return redirect(url_for("cultural_bp.pageant_flow",
                                    enrollment_id=enrollment.id,
                                    category_id=category.id))

    return render_template("program_culturefire/segment_edit.html",
                           enrollment=enrollment, category=category, show=show, submission=submission, segment=segment)

@cultural_bp.route("/talent/pageant/<int:enrollment_id>/<int:category_id>/<segment>/delete", methods=["POST"])
@login_required
def segment_delete(enrollment_id, category_id, segment):
    enrollment = UserEnrollment.query.get_or_404(enrollment_id)
    category = CfiTalentCategoryItem.query.get_or_404(category_id)
    show = CfiShow.query.filter_by(category_item_id=category.id, status="active").first_or_404()

    submission = CfiTalentSubmission.query.filter_by(
        user_enrollment_id=enrollment.id,
        category_item_id=category.id,
        show_id=show.id,
        segment_type=segment
    ).first_or_404()

    db.session.delete(submission)
    db.session.commit()
    flash(f"{segment.replace('_',' ').title()} deleted.", "info")

    return redirect(url_for("cultural_bp.pageant_flow",
                            enrollment_id=enrollment.id,
                            category_id=category.id))
''' 

@cultural_bp.route("/talent/flow/<int:enrollment_id>/<int:category_id>")
@login_required
def talent_flow(enrollment_id, category_id):
    enrollment = UserEnrollment.query.get_or_404(enrollment_id)
    category = CfiTalentCategoryItem.query.get_or_404(category_id)

    # Redirect to the main talent dashboard with the selected category
    return redirect(url_for(
        "cultural_bp.talent_dashboard",
        enrollment_id=enrollment.id,
        category_id=category.id
    ))

'''
@cultural_bp.route("/talent/pageant/<int:enrollment_id>/<int:category_id>/<segment>/new", methods=["GET", "POST"])
@login_required
def segment_new(enrollment_id, category_id, segment):
    enrollment = UserEnrollment.query.get_or_404(enrollment_id)
    category = CfiTalentCategoryItem.query.get_or_404(category_id)
    show = CfiShow.query.filter_by(category_item_id=category.id, status="active").first_or_404()

    if request.method == "POST":
        file = request.files.get("video")
        if not file:
            flash(f"Please upload a {segment} video.", "error")
            return redirect(request.url)

        filename = secure_filename(file.filename)
        filepath = os.path.join(os.path.join(current_app.root_path, "static", "uploads"), filename)
        file.save(filepath)

        submission = CfiTalentSubmission(
            user_enrollment_id=enrollment.id,
            category_item_id=category.id,
            show_id=show.id,
            segment_type=segment,
            video_url=filepath
        )
        db.session.add(submission)
        db.session.commit()

        if all_segments_filled(show):
            show.status = "completed"
            db.session.commit()
            flash(f"{show.title} is now marked completed!", "success")

        return redirect(url_for("cultural_bp.pageant_flow",
                                enrollment_id=enrollment.id,
                                category_id=category.id))

    return render_template("program_culturefire/segment_form.html",
                           enrollment=enrollment, category=category, show=show, segment=segment)

@cultural_bp.route("/talent/pageant/<int:enrollment_id>/<int:category_id>/ramp_walk", methods=["GET", "POST"])
@login_required
def ramp_walk(enrollment_id, category_id):
    enrollment = UserEnrollment.query.get_or_404(enrollment_id)
    category = CfiTalentCategoryItem.query.get_or_404(category_id)
    show = CfiShow.query.filter_by(category_item_id=category.id, status="active").first_or_404()

    if request.method == "POST":
        file = request.files.get("video")
        if not file:
            flash("Please upload a Ramp Walk + Intro video.", "error")
            return redirect(request.url)

        filename = secure_filename(file.filename)
        filepath = os.path.join(os.path.join(current_app.root_path, "static", "uploads"), filename)
        file.save(filepath)

        submission = CfiTalentSubmission(
            user_enrollment_id=enrollment.id,
            category_item_id=category.id,
            show_id=show.id,
            segment_type="ramp_walk",
            video_url=filepath
        )
        db.session.add(submission)
        db.session.commit()

        if all_segments_filled(show):
            show.status = "completed"
            db.session.commit()

        return redirect(url_for("cultural_bp.pageant_dashboard",
                                enrollment_id=enrollment.id,
                                category_id=category.id))

    return render_template("program_culturefire/segments/ramp_walk.html",
                           enrollment=enrollment, category=category, show=show)
 

@cultural_bp.route(
    "/talent/pageant/select_segment/<int:enrollment_id>/<int:show_id>/<int:category_id>",
    methods=["GET", "POST"]
)
@login_required
def select_segment(enrollment_id, show_id, category_id):
    form = SegmentSelectForm()

    segments = [s for s in CfiPageantSegment.query.all() if s.name.lower() not in ('sponsor', 'supporter')]
    form.segment_id.choices = [(s.id, s.name) for s in segments]

    if form.validate_on_submit():
        return redirect(url_for(
            "cultural_bp.segment_form",
            enrollment_id=enrollment_id,
            show_id=show_id,
            category_id=category_id,
            segment_id=form.segment_id.data
        ))

    return render_template(
        "program_culturefire/select_segment.html",
        form=form,
        enrollment=UserEnrollment.query.get_or_404(enrollment_id),
        show=CfiShow.query.get_or_404(show_id),
        category=CfiTalentCategoryItem.query.get_or_404(category_id),
        segments=segments
    )
'''

@cultural_bp.route("/talent/pageant/<int:enrollment_id>/<int:category_id>", methods=["GET"])
@login_required
def pageant_dashboard(enrollment_id, category_id):
    enrollment = UserEnrollment.query.get_or_404(enrollment_id)
    category = CfiTalentCategoryItem.query.get_or_404(category_id)

    shows = CfiShow.query.filter_by(
        category_item_id=category.id
    ).order_by(CfiShow.id.asc()).all()

    show = next((s for s in shows if s.status != "completed"), None)

    if not show:
        show = CfiShow(
            title=f"{category.name} Show {len(shows)+1}",
            description=f"{category.name} showcase",
            start_date=date.today(),
            location="TBD",
            status="active",
            category_item_id=category.id
        )
        db.session.add(show)
        db.session.commit()

    segments = [s for s in CfiPageantSegment.query.all() if s.name.lower() not in ('sponsor', 'supporter')]

    submissions = CfiSegmentItem.query.filter_by(
        enrollment_id=enrollment.id,
        show_id=show.id
    ).all()

    return render_template(
        "program_culturefire/pageant_dashboard.html",
        enrollment=enrollment,
        category=category,
        show=show,
        segments=segments,
        submissions=submissions
    )

@cultural_bp.route(
    "/talent/pageant/segment_form/<int:enrollment_id>/<int:show_id>/<int:category_id>",
    methods=["GET", "POST"]
)
@login_required
def segment_form(enrollment_id, show_id, category_id):

    form = PageantForm()

    segments = [s for s in CfiPageantSegment.query.all() if s.name.lower() not in ('sponsor', 'supporter')]
    form.segment_id.choices = [(s.id, s.name) for s in segments]

    enrollment = UserEnrollment.query.get_or_404(enrollment_id)
    show = CfiShow.query.get_or_404(show_id)
    category = CfiTalentCategoryItem.query.get_or_404(category_id)

    # ✅ single source of truth
    segment_id = request.args.get("segment_id", type=int) or form.segment_id.data

    segment = None
    if segment_id:
        segment = CfiPageantSegment.query.get_or_404(segment_id)

    submission = None
    if segment:
        submission = CfiSegmentItem.query.filter_by(
            enrollment_id=enrollment_id,
            show_id=show_id,
            segment_type=segment.name
        ).first()

    if request.method == "POST":
        file = request.files.get("video")
        if file and file.filename:
            cfi_upload_folder = os.path.join(os.path.join(current_app.root_path, "static", "uploads"), "cfi")
            os.makedirs(cfi_upload_folder, exist_ok=True)

            filename = secure_filename(file.filename)
            filepath = os.path.join(cfi_upload_folder, filename)
            file.save(filepath)

            if submission:
                submission.video_url = f"cfi/{filename}"
                submission.status = "uploaded"
            else:
                submission = CfiSegmentItem(
                    enrollment_id=enrollment_id,
                    show_id=show_id,
                    segment_type=segment.name,
                    title=segment.name,
                    video_url=f"cfi/{filename}",
                    status="uploaded"
                )
                db.session.add(submission)

            db.session.commit()
            flash("Video uploaded successfully!", "success")
            return redirect(url_for(
                "cultural_bp.segment_form",
                enrollment_id=enrollment_id,
                show_id=show_id,
                category_id=category_id,
                segment_id=segment_id
            ))

    all_submissions = CfiSegmentItem.query.filter_by(
        enrollment_id=enrollment_id,
        show_id=show_id
    ).all()

    next_segment = None
    if segment and segments:
        for i, s in enumerate(segments):
            if s.id == segment.id:
                if i + 1 < len(segments):
                    next_segment = segments[i+1]
                break

    return render_template(
        "program_culturefire/segment_form.html",
        form=form,
        enrollment=enrollment,
        show=show,
        category=category,
        segments=segments,
        segment=segment,
        submission=submission,
        submissions=all_submissions,
        next_segment=next_segment
    )

@cultural_bp.route("/talent/dashboard/<int:enrollment_id>", methods=["GET"])
@login_required
def talent_dashboard(enrollment_id):
    enrollment = UserEnrollment.query.get_or_404(enrollment_id)
    categories = CfiTalentCategoryItem.query.all()

    # Read selected category/segment from query string
    #selected_category_id = request.args.get("category_id", type=int)
    selected_category_id = request.args.get("category_id", type=int)

    selected_segment_id = request.args.get("segment_id", type=int)

    selected_category = None
    segments = []
    show = None
    segment_items = []

    if selected_category_id:
        selected_category = CfiTalentCategoryItem.query.get(selected_category_id)
        if selected_category and selected_category.name == "Pageant":
            segments = [s for s in CfiPageantSegment.query.all() if s.name.lower() not in ('sponsor', 'supporter')]
            show = CfiShow.query.filter_by(category_item_id=selected_category.id).first()
            if show:
                segment_items = CfiSegmentItem.query.filter_by(
                    enrollment_id=enrollment.id,
                    show_id=show.id
                ).all()

    talents_all = CfiTalentSubmission.query.filter_by(
        user_id=enrollment.user_id,
        subject_id=enrollment.subject_id
    ).order_by(CfiTalentSubmission.id.desc()).all()
    
    show_all = request.args.get("all", "false").lower() == "true"
    talents = talents_all if show_all else talents_all[:3]

    return render_template(
        "program_culturefire/talent_dashboard.html",
        enrollment=enrollment,
        categories=categories,
        selected_category_id=selected_category_id,
        selected_category=selected_category,
        selected_segment_id=selected_segment_id,
        segments=segments,
        segment_items=segment_items,
        show=show,
        talents=talents,
        show_all=show_all,
        showing_count=len(talents),
        total_count=len(talents_all)
    )


@cultural_bp.route("/show/vote", methods=["POST"])
@login_required
def vote_item():
    data = request.json
    sub_id = data.get("submission_id")
    v_type = data.get("type", "talent")
    score = int(data.get("score", 0))

    if v_type == "pageant":
        item = CfiSegmentItem.query.get(sub_id)
        if not item:
            return jsonify({"success": False, "message": "Segment item not found"})
        
        if not CfiJudgeAssignment.query.filter_by(show_id=item.show_id, judge_id=current_user.id).first():
            return jsonify({"success": False, "message": "Only assigned judges can score this pageant!"})

        existing_vote = CfiShowcaseVote.query.filter_by(user_id=current_user.id, segment_item_id=sub_id).first()
        if existing_vote:
            existing_vote.score = score
        else:
            vote = CfiShowcaseVote(user_id=current_user.id, segment_item_id=sub_id, score=score)
            db.session.add(vote)
    else:
        sub = CfiTalentSubmission.query.get(sub_id)
        if not sub:
            return jsonify({"success": False, "message": "Submission not found"})
            
        if not CfiJudgeAssignment.query.filter_by(show_id=sub.show_id, judge_id=current_user.id).first():
            return jsonify({"success": False, "message": "Only assigned judges can score this show!"})

        existing_vote = CfiShowcaseVote.query.filter_by(user_id=current_user.id, submission_id=sub_id).first()
        if existing_vote:
            existing_vote.score = score
        else:
            vote = CfiShowcaseVote(user_id=current_user.id, submission_id=sub_id, score=score)
            db.session.add(vote)

    db.session.commit()
    return jsonify({"success": True})

@cultural_bp.route("/show/<int:show_id>/results")
@login_required
def pageant_results(show_id):
    show = CfiShow.query.get_or_404(show_id)
    if not show.category_item or show.category_item.name != "Pageant":
        flash("Results are only available for Pageants.", "error")
        return redirect(url_for("cultural_bp.showcase_dashboard"))

    # Tally votes for Pageant Segments
    # We group by segment_item.enrollment_id to get total votes per contestant
    segment_items = CfiSegmentItem.query.filter_by(show_id=show_id).all()
    
    contestant_votes = {}
    segment_types = set()
    
    for item in segment_items:
        enrollment = item.enrollment
        if not enrollment:
            continue
            
        eid = enrollment.id
        if eid not in contestant_votes:
            contestant_votes[eid] = {
                'enrollment': enrollment,
                'name': enrollment.biodata.full_name if enrollment.biodata else "Unknown",
                'total_votes': 0,
                'segments': {}
            }
            
        # Keep track of all segment types used
        segment_type_formatted = item.segment_type.replace('_', ' ').title()
        segment_types.add(segment_type_formatted)
            
        # Sum scores for this segment item
        votes_records = CfiShowcaseVote.query.filter_by(segment_item_id=item.id).all()
        score_sum = sum(v.score for v in votes_records)
        contestant_votes[eid]['total_votes'] += score_sum
        contestant_votes[eid]['segments'][segment_type_formatted] = score_sum

    # Sort contestants by total votes descending
    ranked_contestants = sorted(contestant_votes.values(), key=lambda x: x['total_votes'], reverse=True)
    
    # Sort segment types according to PAGEANT_ORDER
    PAGEANT_ORDER = {
        "ramp_walk": 1,
        "intro": 2,
        "talent": 3,
        "traditional_wear": 4,
        "formal_wear": 5,
        "q_a": 6,
        "q&a": 6,
        "qna": 6
    }
    ordered_segments = sorted(list(segment_types), key=lambda x: PAGEANT_ORDER.get(x.replace(' ', '_').lower(), 99))
    
    # Calculate if the show has ended dynamically based on judging completion
    judge_count = CfiJudgeAssignment.query.filter_by(show_id=show.id).count()
    segment_count = len(segment_items)
    expected_scores = judge_count * segment_count
    
    actual_scores = 0
    for item in segment_items:
        actual_scores += CfiShowcaseVote.query.filter_by(segment_item_id=item.id).count()

    has_ended = (actual_scores >= expected_scores) and (expected_scores > 0)
    
    origin = request.args.get('origin')
    enrollment_id = request.args.get('enrollment_id', type=int)

    return render_template(
        "program_culturefire/pageant_results.html",
        show=show,
        ranked_contestants=ranked_contestants,
        ordered_segments=ordered_segments,
        has_ended=has_ended,
        origin=origin,
        enrollment_id=enrollment_id
    )


@cultural_bp.route("/show/<int:show_id>/winners")
@login_required
def pageant_winners(show_id):
    show = CfiShow.query.get_or_404(show_id)
    if not show.category_item or show.category_item.name != "Pageant":
        flash("Winners are only available for Pageants.", "error")
        return redirect(url_for("cultural_bp.showcase_dashboard"))

    segment_items = CfiSegmentItem.query.filter_by(show_id=show_id).all()
    contestant_votes = {}
    
    for item in segment_items:
        enrollment = item.enrollment
        if not enrollment:
            continue
            
        eid = enrollment.id
        if eid not in contestant_votes:
            contestant_votes[eid] = {
                'enrollment': enrollment,
                'name': enrollment.biodata.full_name if enrollment.biodata else "Unknown",
                'total_votes': 0
            }
            
        votes_records = CfiShowcaseVote.query.filter_by(segment_item_id=item.id).all()
        score_sum = sum(v.score for v in votes_records)
        contestant_votes[eid]['total_votes'] += score_sum

    ranked_contestants = sorted(contestant_votes.values(), key=lambda x: x['total_votes'], reverse=True)
    top_3 = ranked_contestants[:3]
    
    # Optional logic: only allow viewing if ended
    judge_count = CfiJudgeAssignment.query.filter_by(show_id=show.id).count()
    segment_count = len(segment_items)
    expected_scores = judge_count * segment_count
    
    actual_scores = 0
    for item in segment_items:
        actual_scores += CfiShowcaseVote.query.filter_by(segment_item_id=item.id).count()

    has_ended = (actual_scores >= expected_scores) and (expected_scores > 0)
    
    origin = request.args.get('origin')
    enrollment_id = request.args.get('enrollment_id', type=int)

    return render_template(
        "program_culturefire/pageant_winners.html",
        show=show,
        top_3=top_3,
        has_ended=has_ended,
        origin=origin,
        enrollment_id=enrollment_id
    )

@cultural_bp.route("/judge/dashboard")
@login_required
def judge_dashboard():
    # Handle incoming origin data for dynamic back button
    origin = request.args.get('origin')
    enrollment_id = request.args.get('enrollment_id', type=int)
    
    if origin:
        session['cfi_judge_origin'] = origin
    if enrollment_id:
        session['cfi_judge_origin_enrollment'] = enrollment_id
        
    back_origin = session.get('cfi_judge_origin')
    back_enrollment_id = session.get('cfi_judge_origin_enrollment')

    # Handle incoming target_show_id for auto-assignment
    target_show_id = request.args.get('target_show_id', type=int)
    if target_show_id:
        session['cfi_judge_target_show'] = target_show_id
        
    target_from_session = session.get('cfi_judge_target_show')

    # Find all paid applications (enrollments for 'cfi_judge')
    from app.models.auth import AuthSubject, UserEnrollment
    
    judge_subject = AuthSubject.query.filter_by(slug='cfi_judge').first()
    if not judge_subject:
        flash("Judge application system is currently unavailable.", "error")
        return redirect(url_for('bridge_bp.bridge'))
        
    paid_tokens = UserEnrollment.query.filter(
        UserEnrollment.user_id == current_user.id,
        UserEnrollment.subject_id == judge_subject.id,
        UserEnrollment.status.in_(['paid', 'active'])
    ).all()
    
    # Auto-assignment logic if they have a token and a target show
    if paid_tokens and target_from_session:
        show = CfiShow.query.get(target_from_session)
        if show and show.status == 'active':
            current_judges = CfiJudgeAssignment.query.filter_by(show_id=show.id).count()
            is_pageant = show.category_item and show.category_item.name == 'Pageant'
            max_judges = 5 if is_pageant else 3
            
            already_judge = CfiJudgeAssignment.query.filter_by(show_id=show.id, judge_id=current_user.id).first()
            is_participant = CfiTalentSubmission.query.filter_by(show_id=show.id, user_id=current_user.id).first()
            
            if not already_judge and not is_participant and current_judges < max_judges:
                # We can auto-assign!
                token = paid_tokens[0]
                token.status = 'completed'
                new_assignment = CfiJudgeAssignment(
                    judge_id=current_user.id, 
                    show_id=show.id, 
                    role="paid_judge", 
                    enrollment_id=token.id
                )
                db.session.add(new_assignment)
                db.session.commit()
                
                # Clear session
                session.pop('cfi_judge_target_show', None)
                flash(f"You have been successfully assigned as a judge for '{show.title}'!", "success")
                return redirect(url_for('cultural_bp.judge_dashboard'))
            elif current_judges >= max_judges:
                flash(f"Unfortunately, '{show.title}' has reached its judge limit. Please select another show below.", "warning")
                session.pop('cfi_judge_target_show', None)
                # continue to render selection state
                
    # Check if they are currently assigned to any active shows
    active_assignments = CfiJudgeAssignment.query.join(CfiShow).filter(
        CfiJudgeAssignment.judge_id == current_user.id,
        CfiShow.status == 'active'
    ).all()
    
    # If they have no paid tokens and no active assignments, show purchase option
    if not paid_tokens and not active_assignments:
        return render_template("program_culturefire/judge_dashboard.html", state="purchase", subject=judge_subject, back_origin=back_origin, back_enrollment_id=back_enrollment_id)
        
    # If they are already assigned, redirect them to the actual showcase dashboard (or show a link)
    if active_assignments:
        return render_template("program_culturefire/judge_dashboard.html", state="assigned", assignments=active_assignments, back_origin=back_origin, back_enrollment_id=back_enrollment_id)
        
    # If they have paid tokens, show available shows
    # Only active shows that have slots available
    shows = CfiShow.query.filter_by(status='active').all()
    available_shows = []
    
    for show in shows:
        current_judges = CfiJudgeAssignment.query.filter_by(show_id=show.id).count()
        # Pageants have limit 5, others 3
        is_pageant = show.category_item and show.category_item.name == 'Pageant'
        max_judges = 5 if is_pageant else 3
        
        # Check if already a judge
        already_judge = CfiJudgeAssignment.query.filter_by(show_id=show.id, judge_id=current_user.id).first()
        is_participant = CfiTalentSubmission.query.filter_by(show_id=show.id, user_id=current_user.id).first()
        
        if not already_judge and not is_participant and current_judges < max_judges:
            available_shows.append({
                'show': show,
                'current_judges': current_judges,
                'max_judges': max_judges
            })
            
    return render_template("program_culturefire/judge_dashboard.html", state="selection", available_shows=available_shows, tokens=len(paid_tokens), back_origin=back_origin, back_enrollment_id=back_enrollment_id)

@cultural_bp.route("/judge/select_show/<int:show_id>", methods=["POST"])
@login_required
def select_show(show_id):
    from app.models.auth import AuthSubject, UserEnrollment
    
    judge_subject = AuthSubject.query.filter_by(slug='cfi_judge').first()
    paid_token = UserEnrollment.query.filter(
        UserEnrollment.user_id == current_user.id,
        UserEnrollment.subject_id == judge_subject.id,
        UserEnrollment.status.in_(['paid', 'active'])
    ).first()
    
    if not paid_token:
        flash("You need a valid Judge Application token to select a show.", "warning")
        return redirect(url_for('cultural_bp.judge_dashboard'))
        
    show = CfiShow.query.get_or_404(show_id)
    
    # Check limits
    current_judges = CfiJudgeAssignment.query.filter_by(show_id=show.id).count()
    is_pageant = show.category_item and show.category_item.name == 'Pageant'
    max_judges = 5 if is_pageant else 3
    
    if current_judges >= max_judges:
        flash("This show has reached its judge limit.", "warning")
        return redirect(url_for('cultural_bp.judge_dashboard'))
        
    # Consume token
    paid_token.status = 'completed'
    
    # Create Assignment
    new_assignment = CfiJudgeAssignment(
        judge_id=current_user.id,
        show_id=show.id,
        role="paid_judge",
        enrollment_id=paid_token.id
    )
    
    db.session.add(new_assignment)
    db.session.commit()
    
    flash(f"You have been assigned as a judge for '{show.title}'!", "success")
    return redirect(url_for('cultural_bp.judge_dashboard'))

@cultural_bp.route("/admin/cultural_fire")
@login_required
def admin_dashboard():
    # Verify admin role
    if not current_user.has_role('admin'):
        flash("Unauthorized access.", "danger")
        return redirect(url_for('auth_bp.bridge_dashboard'))
        
    shows = CfiShow.query.all()
    return render_template("program_culturefire/admin_dashboard.html", shows=shows)

@cultural_bp.route("/show/<int:show_id>/admin_scores")
@login_required
def admin_scores(show_id):
    # Verify admin role OR judge assignment
    is_admin = current_user.has_role('admin')
    is_judge = CfiJudgeAssignment.query.filter_by(show_id=show_id, judge_id=current_user.id).first() is not None
    
    if not is_admin and not is_judge:
        flash("Unauthorized access.", "danger")
        return redirect(url_for('auth_bp.bridge_dashboard'))
        
    show = CfiShow.query.get_or_404(show_id)
    
    # Get all judges assigned to this show
    judges = CfiJudgeAssignment.query.filter_by(show_id=show.id).options(joinedload(CfiJudgeAssignment.judge)).all()
    
    # Get all contestants
    if show.category_item and show.category_item.name == "Pageant":
        contestants_items = CfiSegmentItem.query.filter_by(show_id=show.id).all()
        contestants_by_user = {}
        for item in contestants_items:
            uid = item.enrollment.user_id if item.enrollment else None
            if uid:
                if uid not in contestants_by_user:
                    name = item.enrollment.biodata.full_name if (item.enrollment and item.enrollment.biodata) else "Unknown"
                    contestants_by_user[uid] = {"name": name, "items": []}
                contestants_by_user[uid]["items"].append(item)
                
        # Get all votes for this show
        # Note: Votes are linked via segment_item_id
        item_ids = [item.id for item in contestants_items]
        votes = CfiShowcaseVote.query.filter(CfiShowcaseVote.segment_item_id.in_(item_ids)).all() if item_ids else []
        
        # Build scoresheet: {contestant_id: {judge_id: total_score}}
        scoresheet = {}
        for uid, data in contestants_by_user.items():
            scoresheet[uid] = {"name": data["name"], "scores": {}}
            for j in judges:
                scoresheet[uid]["scores"][j.judge_id] = 0
                
        for vote in votes:
            # find which user this item belongs to
            uid = next((u for u, data in contestants_by_user.items() if any(i.id == vote.segment_item_id for i in data["items"])), None)
            if uid and vote.user_id in scoresheet[uid]["scores"]:
                scoresheet[uid]["scores"][vote.user_id] += vote.score
                
        contestants_list = list(scoresheet.values())
        
    else:
        # Talent show
        submissions = CfiTalentSubmission.query.filter_by(show_id=show.id).all()
        sub_ids = [s.id for s in submissions]
        votes = CfiShowcaseVote.query.filter(CfiShowcaseVote.submission_id.in_(sub_ids)).all() if sub_ids else []
        
        scoresheet = {}
        for sub in submissions:
            name = sub.talent_name or "Unknown"
            if sub.user_enrollment and sub.user_enrollment.biodata:
                name += f" ({sub.user_enrollment.biodata.full_name})"
            scoresheet[sub.id] = {"name": name, "scores": {}}
            for j in judges:
                scoresheet[sub.id]["scores"][j.judge_id] = 0
                
        for vote in votes:
            if vote.submission_id in scoresheet and vote.user_id in scoresheet[vote.submission_id]["scores"]:
                scoresheet[vote.submission_id]["scores"][vote.user_id] += vote.score
                
        contestants_list = list(scoresheet.values())

    return render_template("program_culturefire/admin_scores.html", show=show, judges=judges, contestants=contestants_list)
