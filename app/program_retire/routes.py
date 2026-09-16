from functools import wraps

from flask import abort, flash, g, redirect, render_template, url_for
from flask_login import current_user, login_required
from flask_wtf import FlaskForm
from sqlalchemy.exc import IntegrityError
from wtforms import BooleanField, IntegerField, SelectField, StringField, SubmitField, TextAreaField
from wtforms.validators import DataRequired, InputRequired, Length, NumberRange, Optional

from app.extensions import db
from app.models.retire import (
    RetirementMembership, RetirementOrganisation, RetirementRole, STAFF_ROLE_CODES, utcnow,
)
from . import retire_bp

MODULES = (
    "Compliance & Registration", "Resident Management", "Accommodation",
    "Care & Wellbeing", "Meals & Kitchen", "Accounts", "Staff Management",
    "Operations", "Family / Representative Access", "Management Dashboard",
)


class OrganisationRegistrationForm(FlaskForm):
    name = StringField("Organisation name", filters=[lambda value: value.strip() if value else value],
                       validators=[DataRequired(), Length(max=200)])
    submit = SubmitField("Register Organisation")


class OrganisationLookupForm(FlaskForm):
    organisation_id = IntegerField("Retirement organisation ID", validators=[InputRequired(), NumberRange(min=1)])
    submit = SubmitField("Find organisation")


class JoinForm(FlaskForm):
    role_id = SelectField("Requested role", coerce=int, validators=[InputRequired()])
    confirm = BooleanField("I confirm this is the organisation I want to join", validators=[DataRequired()])
    submit = SubmitField("Request membership")


class ReviewForm(FlaskForm):
    decision = SelectField("Decision", choices=[("approve", "Approve"), ("deny", "Deny")], validators=[DataRequired()])
    role_id = SelectField("Approved staff role (required for approval)", coerce=int, validators=[Optional()])
    reason = TextAreaField("Review reason", filters=[lambda value: value.strip() if value else value], validators=[DataRequired(), Length(max=2000)])
    submit = SubmitField("Save decision")


def _staff_roles():
    return RetirementRole.query.filter(RetirementRole.code.in_(STAFF_ROLE_CODES)).order_by(RetirementRole.id).all()


def _membership(organisation_id):
    return RetirementMembership.query.filter_by(organisation_id=organisation_id, user_id=current_user.id).first()


def _destination(membership):
    endpoint = "dashboard" if membership.status == "active" and membership.approved_role_id else "status"
    return redirect(url_for("retire_bp." + endpoint, organisation_id=membership.organisation_id))


def active_membership_required(view):
    """Membership gate for every future operational endpoint; capabilities are separate."""
    @wraps(view)
    @login_required
    def wrapped(organisation_id, *args, **kwargs):
        membership = _membership(organisation_id)
        if membership is None:
            abort(403)
        if membership.status != "active" or membership.approved_role_id is None:
            return redirect(url_for("retire_bp.status", organisation_id=organisation_id))
        g.retirement_membership = membership
        g.retirement_organisation = membership.organisation
        return view(organisation_id, *args, **kwargs)
    return wrapped


def _require_owner():
    # A role label alone never grants owner authority.
    if g.retirement_organisation.owner_user_id != current_user.id:
        abort(403)


@retire_bp.route("/")
def welcome():
    if current_user.is_authenticated:
        return redirect(url_for("retire_bp.entry"))
    return render_template("program_retire/welcome.html")


@retire_bp.route("/about")
def about():
    return render_template("program_retire/about.html", modules=MODULES)


@retire_bp.route("/entry")
@login_required
def entry():
    memberships = RetirementMembership.query.filter_by(user_id=current_user.id).order_by(RetirementMembership.organisation_id).all()
    if len(memberships) == 1:
        return _destination(memberships[0])
    return render_template("program_retire/entry.html", memberships=memberships)


@retire_bp.route("/register", methods=["GET", "POST"])
@login_required
def register():
    organisation = RetirementOrganisation.query.filter_by(owner_user_id=current_user.id).first()
    if organisation:
        membership = _membership(organisation.id)
        if membership:
            return _destination(membership)
        # Stage 1 owners are backfilled by the proposed Stage 2 migration, never on GET.
        abort(503, description="Retirement owner membership setup is required.")
    form = OrganisationRegistrationForm()
    if form.validate_on_submit():
        owner_role = RetirementRole.query.filter_by(code="organisation_owner").first()
        if owner_role is None:
            abort(503, description="Retirement roles have not been provisioned.")
        try:
            organisation = RetirementOrganisation(name=form.name.data, owner_user_id=current_user.id)
            db.session.add(organisation)
            db.session.flush()
            db.session.add(RetirementMembership(
                organisation_id=organisation.id, user_id=current_user.id, status="active",
                requested_role_id=None, approved_role_id=owner_role.id,
                reviewed_at=None, reviewed_by_user_id=None,
            ))
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            organisation = RetirementOrganisation.query.filter_by(owner_user_id=current_user.id).first()
            if organisation and _membership(organisation.id):
                return _destination(_membership(organisation.id))
            raise
        except Exception:
            # A flush is not a commit: discard all registration work on failure.
            db.session.rollback()
            raise
        flash("Organisation registered successfully.", "success")
        return redirect(url_for("retire_bp.dashboard", organisation_id=organisation.id))
    return render_template("program_retire/register.html", form=form)


@retire_bp.route("/join", methods=["GET", "POST"])
@login_required
def join():
    form = OrganisationLookupForm()
    if form.validate_on_submit():
        organisation = db.session.get(RetirementOrganisation, form.organisation_id.data)
        if organisation is None:
            form.organisation_id.errors.append("No Retirement organisation has this ID.")
        else:
            return redirect(url_for("retire_bp.request_membership", organisation_id=organisation.id))
    return render_template("program_retire/join.html", form=form, organisation=None)


@retire_bp.route("/organisations/<int:organisation_id>/join", methods=["GET", "POST"])
@login_required
def request_membership(organisation_id):
    organisation = db.get_or_404(RetirementOrganisation, organisation_id)
    existing = _membership(organisation_id)
    if existing:
        return _destination(existing)
    if organisation.owner_user_id == current_user.id:
        abort(409, description="Owner membership must be established during organisation setup.")
    form = JoinForm()
    form.role_id.choices = [(role.id, role.name) for role in _staff_roles()]
    if form.validate_on_submit():
        role = db.session.get(RetirementRole, form.role_id.data)
        if role is None or role.code not in STAFF_ROLE_CODES:
            abort(400)
        try:
            db.session.add(RetirementMembership(
                organisation_id=organisation_id, user_id=current_user.id,
                requested_role_id=role.id, approved_role_id=None, status="pending",
            ))
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            if not _membership(organisation_id):
                raise
        return _destination(_membership(organisation_id))
    return render_template("program_retire/join.html", form=form, organisation=organisation)


@retire_bp.route("/organisations/<int:organisation_id>/status")
@login_required
def status(organisation_id):
    membership = _membership(organisation_id)
    if membership is None:
        abort(404)
    if membership.status == "active" and membership.approved_role_id:
        return _destination(membership)
    return render_template("program_retire/status.html", membership=membership)


@retire_bp.route("/dashboard")
@login_required
def dashboard_entry():
    return redirect(url_for("retire_bp.entry"))


@retire_bp.route("/organisations/<int:organisation_id>/dashboard")
@active_membership_required
def dashboard(organisation_id):
    return render_template("program_retire/dashboard.html", organisation=g.retirement_organisation,
                           modules=MODULES, is_owner=g.retirement_organisation.owner_user_id == current_user.id)


@retire_bp.route("/organisations/<int:organisation_id>/members/pending")
@active_membership_required
def pending_members(organisation_id):
    _require_owner()
    members = RetirementMembership.query.filter_by(organisation_id=organisation_id, status="pending").order_by(RetirementMembership.requested_at, RetirementMembership.id).all()
    return render_template("program_retire/pending.html", organisation=g.retirement_organisation, members=members)


@retire_bp.route("/organisations/<int:organisation_id>/members/<int:membership_id>/review", methods=["GET", "POST"])
@active_membership_required
def review_member(organisation_id, membership_id):
    _require_owner()
    # Lock and scope the target so concurrent/repeated reviews cannot overwrite a decision.
    membership = RetirementMembership.query.filter_by(id=membership_id, organisation_id=organisation_id).with_for_update().first_or_404()
    if membership.user_id == current_user.id:
        abort(403)
    if membership.status != "pending":
        abort(409, description="This application has already been reviewed.")
    form = ReviewForm()
    form.role_id.choices = [(0, "Select a staff role")] + [(role.id, role.name) for role in _staff_roles()]
    if form.validate_on_submit():
        role = db.session.get(RetirementRole, form.role_id.data) if form.role_id.data else None
        if form.decision.data == "approve" and (role is None or role.code not in STAFF_ROLE_CODES):
            form.role_id.errors.append("Choose a permitted staff role.")
        else:
            membership.status = "active" if form.decision.data == "approve" else "denied"
            membership.approved_role_id = role.id if membership.status == "active" else None
            membership.reviewed_by_user_id = current_user.id
            membership.reviewed_at = utcnow()
            membership.review_reason = form.reason.data.strip()
            db.session.commit()
            flash("Membership decision recorded.", "success")
            return redirect(url_for("retire_bp.pending_members", organisation_id=organisation_id))
    return render_template("program_retire/review.html", organisation=g.retirement_organisation, membership=membership, form=form)
