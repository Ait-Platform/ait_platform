from flask import Blueprint, redirect, render_template, url_for, request, flash
from flask_login import login_required, current_user
from app.extensions import db
from app.models.tpx import TPXPassport, TPXEmployment, TPXQualification, TPXSkill
from .forms import CandidateProfileForm, WorkExperienceForm, EducationForm, SkillForm

tpx_bp = Blueprint(
    "tpx_bp",
    __name__,
    template_folder="templates"
)

@tpx_bp.route("/welcome")
def welcome():
    return render_template("program_tpx/welcome.html")

@tpx_bp.route("/about")
def about():
    return render_template("program_tpx/about.html")

@tpx_bp.route("/how-it-works")
def how_it_works():
    return render_template("program_tpx/how_it_works.html")

@tpx_bp.route("/register")
def register_choice():
    return redirect(url_for("auth_bp.register"))

@tpx_bp.route("/pricing")
def pricing():
    from flask import request, render_template
    from app.payments.pricing import price_for_country
    from app.models.auth import AuthSubject
    
    country_code = request.args.get('country') or request.headers.get("CF-IPCountry", "ZA")
    tpx_subject = AuthSubject.query.filter_by(slug='tpx').first()
    
    display_price = "ZAR 100"
    if tpx_subject:
        row = price_for_country(tpx_subject.id, country_code)
        if row:
            # row returns (local_amount_cents, zar_amount_cents, currency)
            local_amt = row[0] / 100.0
            currency = row[2]
            display_price = f"{currency} {local_amt:,.2f}"
            
    return render_template("program_tpx/pricing.html", display_price=display_price)

#price = price_for_country(subject_id, country_code)'quote_bp.quote', subject='tpx'

@tpx_bp.route("/quote")
@login_required
def quote():
    return render_template("program_tpx/quote.html")

@tpx_bp.route("/dashboard")
@login_required
def dashboard():
    passport = TPXPassport.query.filter_by(user_id=current_user.id).first()
    return render_template("program_tpx/dashboard.html", passport=passport)

@tpx_bp.route("/profile", methods=["GET", "POST"])
@login_required
def profile():
    passport = TPXPassport.query.filter_by(user_id=current_user.id).first()
    form = CandidateProfileForm(obj=candidate)
    
    if form.validate_on_submit():
        if not passport:
            passport = TPXPassport(user_id=current_user.id)
            db.session.add(candidate)
        
        passport.first_name = form.first_name.data
        passport.last_name = form.last_name.data
        passport.headline = form.headline.data
        passport.summary = form.summary.data
        
        db.session.commit()
        flash("Profile updated successfully!", "success")
        return redirect(url_for("tpx_bp.dashboard"))
        
    return render_template("program_tpx/profile.html", form=form, passport=candidate)


from app.models.tpx import TPXEmployment, TPXQualification, TPXSkill

@tpx_bp.route("/work-experience", methods=["GET", "POST"])
@login_required
def work_experience():
    passport = TPXPassport.query.filter_by(user_id=current_user.id).first()
    if not passport:
        flash("Please complete your profile first.", "warning")
        return redirect(url_for("tpx_bp.profile"))
        
    form = WorkExperienceForm()
    if form.validate_on_submit():
        exp = TPXEmployment(
            passport_id=passport.id,
            job_title=form.job_title.data,
            company=form.company.data,
            location=form.location.data,
            start_date=form.start_date.data,
            end_date=form.end_date.data,
            description=form.description.data
        )
        db.session.add(exp)
        db.session.commit()
        flash("Work experience added!", "success")
        return redirect(url_for("tpx_bp.work_experience"))
        
    experiences = TPXEmployment.query.filter_by(passport_id=passport.id).order_by(TPXEmployment.created_at.desc()).all()
    return render_template("program_tpx/work_experience.html", form=form, experiences=experiences)

@tpx_bp.route("/education", methods=["GET", "POST"])
@login_required
def education():
    passport = TPXPassport.query.filter_by(user_id=current_user.id).first()
    if not passport:
        flash("Please complete your profile first.", "warning")
        return redirect(url_for("tpx_bp.profile"))
        
    form = EducationForm()
    if form.validate_on_submit():
        edu = TPXQualification(
            passport_id=passport.id,
            degree=form.degree.data,
            institution=form.institution.data,
            graduation_year=form.graduation_year.data
        )
        db.session.add(edu)
        db.session.commit()
        flash("Education added!", "success")
        return redirect(url_for("tpx_bp.education"))
        
    educations = TPXQualification.query.filter_by(passport_id=passport.id).order_by(TPXQualification.created_at.desc()).all()
    return render_template("program_tpx/education.html", form=form, educations=educations)

@tpx_bp.route("/skills", methods=["GET", "POST"])
@login_required
def skills():
    passport = TPXPassport.query.filter_by(user_id=current_user.id).first()
    if not passport:
        flash("Please complete your profile first.", "warning")
        return redirect(url_for("tpx_bp.profile"))
        
    form = SkillForm()
    if form.validate_on_submit():
        skill = TPXSkill(
            passport_id=passport.id,
            skill_name=form.skill_name.data
        )
        db.session.add(skill)
        db.session.commit()
        flash("Skill added!", "success")
        return redirect(url_for("tpx_bp.skills"))
        
    skills = TPXSkill.query.filter_by(passport_id=passport.id).all()
    return render_template("program_tpx/skills.html", form=form, skills=skills)
