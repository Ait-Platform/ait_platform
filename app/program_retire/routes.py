from functools import wraps

from flask import abort, flash, g, redirect, render_template, url_for, request
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
    authority = BooleanField("I am authorised to establish and administer this new retirement home on RCM", validators=[DataRequired()])
    submit = SubmitField("Create new retirement home")


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
    from .authority import is_owner, staff_eligible
    endpoint = "dashboard" if is_owner(membership) or staff_eligible(membership) else "status"
    return redirect(url_for("retire_bp." + endpoint, organisation_id=membership.organisation_id))


def active_membership_required(view):
    """Membership gate for every future operational endpoint; capabilities are separate."""
    @wraps(view)
    @login_required
    def wrapped(organisation_id, *args, **kwargs):
        membership = _membership(organisation_id)
        if membership is None:
            abort(403)
        from .authority import is_owner, staff_eligible
        if not (is_owner(membership) or staff_eligible(membership)):
            return redirect(url_for("retire_bp.status", organisation_id=organisation_id))
        g.retirement_membership = membership
        g.retirement_organisation = membership.organisation
        return view(organisation_id, *args, **kwargs)
    return wrapped


def _require_owner():
    # A role label alone never grants owner authority.
    from .authority import is_owner
    if g.retirement_organisation.owner_user_id != current_user.id or not is_owner(g.retirement_membership):
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
                association_approved_at=utcnow(), association_approved_by_user_id=current_user.id,
            ))
            db.session.commit()
        except Exception:
            # A flush is not a commit: discard all registration work on failure.
            db.session.rollback()
            raise
        flash("Organisation registered successfully.", "success")
        return redirect(url_for("retire_bp.owner_setup", organisation_id=organisation.id))
    return render_template("program_retire/register.html", form=form)


@retire_bp.route("/join", methods=["GET", "POST"])
@login_required
def join():
    if request.method == 'POST':
        abort(409, description="Legacy join requests are closed. Use the RCM Waiting Room.")
    return redirect(url_for('retire_bp.waiting_room'))


@retire_bp.route("/organisations/<int:organisation_id>/join", methods=["GET", "POST"])
@login_required
def request_membership(organisation_id):
    if request.method == 'POST':
        abort(409, description="Legacy role requests are closed. Use the RCM Waiting Room.")
    existing = _membership(organisation_id)
    return _destination(existing) if existing else redirect(url_for('retire_bp.waiting_room'))


@retire_bp.route("/organisations/<int:organisation_id>/status")
@login_required
def status(organisation_id):
    membership = _membership(organisation_id)
    if membership is None:
        abort(404)
    from .authority import is_owner, staff_eligible
    if is_owner(membership) or staff_eligible(membership):
        return _destination(membership)
    return render_template("program_retire/status.html", membership=membership)


@retire_bp.route("/dashboard")
@login_required
def dashboard_entry():
    return redirect(url_for("retire_bp.entry"))


@retire_bp.route("/organisations/<int:organisation_id>/dashboard")
@active_membership_required
def dashboard(organisation_id):
    from .authority import staff_eligible, is_owner
    return render_template("program_retire/dashboard.html", organisation=g.retirement_organisation,
                           modules=MODULES, is_owner=is_owner(g.retirement_membership),
                           has_staff_authority=staff_eligible(g.retirement_membership))


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
    membership = RetirementMembership.query.filter_by(id=membership_id, organisation_id=organisation_id).first_or_404()
    if request.method == 'POST':
        abort(409, description="Legacy role decisions are closed. Use relationship management after association approval.")
    return render_template('program_retire/legacy_review.html', organisation=g.retirement_organisation, membership=membership)



@retire_bp.route("/onboarding")
def onboarding():
    return render_template("program_retire/onboarding.html")


@retire_bp.route("/other")
@login_required
def other():
    return redirect(url_for("retire_bp.waiting_room"))


@retire_bp.route("/organisations/<int:organisation_id>/setup")
@active_membership_required
def owner_setup(organisation_id):
    _require_owner()
    return render_template("program_retire/setup.html", organisation=g.retirement_organisation)


from . import waiting, authority  # noqa: E402, F401
