from datetime import datetime
import random
import uuid

from flask import abort, flash, g, redirect, render_template, request, url_for
from flask_login import current_user, login_required
from sqlalchemy.exc import IntegrityError

from app.extensions import db
from app.models.auth import User
from app.models.core import CoreInteraction, CoreOrganizationMember, CoreRole, CoreRoleAssignment, CoreTask
from app.models.uip import UipMunicipalReferral, UipProvider, UipWorkOrder
from app.uip import uip_bp
from app.uip.gateway import LunaGateway
from app.uip.services import audit, register, providers, work_orders, operations
from app.uip.services import routing, sla, reception
from app.uip.services.dashboard import metrics
from app.models.uip import (UipMemberProfile, UipProperty, UipPropertyMember,
    UipMemberRepresentative, UipCommunicationPreference)


STAFF_ROLES = ("manager", "receptionist")
CATEGORIES = {"GENERAL ENQUIRY", "SERVICE ISSUE", "SECURITY", "CLEANING",
              "MAINTENANCE", "MUNICIPAL SERVICE", "COMMUNITY MATTER", "FINANCIAL"}
CHANNELS = {"Telephone", "Reception", "Email", "Web", "WhatsApp"}
PRIORITIES = {"LOW", "NORMAL", "HIGH", "URGENT"}


def _require_role(*allowed):
    """Require active membership and an eligible role within this organisation."""
    membership = CoreOrganizationMember.query.filter_by(
        organization_id=g.organization.id, user_id=current_user.id, is_active=True
    ).first()
    if not membership:
        abort(403)
    query = CoreRoleAssignment.query.filter_by(
        organization_id=g.organization.id, user_id=current_user.id
    )
    if allowed:
        query = query.filter(CoreRoleAssignment.role.has(db.and_(CoreRole.slug.in_(allowed),
            db.or_(CoreRole.organization_id.is_(None), CoreRole.organization_id == g.organization.id))))
    query = query.filter(CoreRoleAssignment.role.has(db.or_(
        CoreRole.organization_id.is_(None), CoreRole.organization_id == g.organization.id)))
    assignments = query.all()
    roles = [assignment.role.slug for assignment in assignments if assignment.role]
    if allowed:
        # Staff privileges take precedence over a second resident/provider role.
        roles = [role for role in allowed if role in roles]
    if not roles:
        abort(403)
    return roles[0]


def _members_with_roles(*roles):
    return User.query.join(
        CoreOrganizationMember, CoreOrganizationMember.user_id == User.id
    ).join(
        CoreRoleAssignment, CoreRoleAssignment.user_id == User.id
    ).filter(
        CoreOrganizationMember.organization_id == g.organization.id,
        CoreOrganizationMember.is_active.is_(True),
        CoreRoleAssignment.organization_id == g.organization.id,
        CoreRoleAssignment.role.has(db.and_(CoreRole.slug.in_(roles),
            db.or_(CoreRole.organization_id.is_(None), CoreRole.organization_id == g.organization.id)))
    ).distinct()


def _interaction(reference):
    return CoreInteraction.query.filter_by(
        organization_id=g.organization.id, reference=reference
    ).first_or_404()


def _positive_id(value):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        abort(400, description="Invalid record ID.")
    if parsed <= 0:
        abort(400, description="Invalid record ID.")
    return parsed


@uip_bp.route("/<org_slug>/dashboard")
@login_required
def dashboard(org_slug):
    org = g.organization
    role_slug = _require_role("manager", "receptionist", "committee_member", "owner", "resident", "provider")
    if role_slug == "provider":
        return redirect(url_for("uip_bp.work_order_list", org_slug=org_slug))
    if role_slug in {"resident", "owner"}:
        interactions = CoreInteraction.query.filter_by(
            organization_id=org.id, creator_id=current_user.id
        ).all()
        return render_template("uip/dashboards/resident.html", org=org, interactions=interactions)
    if role_slug == "receptionist":
        from app.uip.presentation import issue_rows
        rows = [row for row in issue_rows(org.id, current_user.id) if "open" in row["filters"]]
        return render_template("uip/dashboards/receptionist.html", org=org, issue_rows=rows)
    if role_slug == "committee_member":
        from app.uip.services.governance import overview
        return render_template("uip/dashboards/committee.html", org=org, governance_metrics=overview(org.id, current_user.id))
    if role_slug == "manager":
        from app.uip.presentation import executive
        return render_template("uip/dashboards/manager.html", org=org, overview=executive(org.id, current_user.id))
    abort(403)


@uip_bp.route("/<org_slug>/settings", methods=["GET", "POST"])
@login_required
def org_settings(org_slug):
    _require_role("manager", "committee_member", "owner")
    org = g.organization
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        status = request.form.get("status")
        if not name or len(name) > 255 or status not in {"active", "inactive"}:
            abort(400, description="Invalid organisation name or status.")
        values = {}
        for field, limit in (("area", 255), ("municipality_ref", 255),
                             ("contact_email", 255), ("contact_phone", 50)):
            values[field] = (request.form.get(field) or "").strip()
            if len(values[field]) > limit:
                abort(400, description="Organisation detail is too long.")
        org.name, org.status = name, status
        for field, value in values.items():
            setattr(org, field, value)
        audit.record(org.id, current_user.id, "organization.updated", org)
        db.session.commit()
        flash("Organisation settings updated successfully.", "success")
        return redirect(url_for("uip_bp.org_settings", org_slug=org_slug))
    return render_template("uip/admin/settings.html", org=org)


@uip_bp.route("/<org_slug>/interaction/new", methods=["GET", "POST"])
@login_required
def new_interaction(org_slug):
    _require_role("manager", "receptionist", "committee_member")
    org = g.organization
    residents = _members_with_roles("resident").all()
    register_members = register.members(org.id, current_user.id, active_only=True).all()
    register_properties = register.properties(org.id, current_user.id, active_only=True).all()
    selected_member, selected_property = register.intake_links(org.id, current_user.id,
        request.args.get("member_id"), request.args.get("property_id"))
    g.intake_member_id = selected_member.id if selected_member else None
    g.intake_property_id = selected_property.id if selected_property else None
    from app.uip.presentation import current_relationship
    g.intake_property_members = {}
    for relationship in UipPropertyMember.query.filter_by(organization_id=org.id).all():
        if current_relationship(relationship):
            g.intake_property_members.setdefault(relationship.property_id, []).append(relationship.member_id)
    if selected_member:
        register_properties.sort(key=lambda p: selected_member.id not in g.intake_property_members.get(p.id, []))
    if request.method == "POST":
        email = (request.form.get("resident_email") or "").strip()
        resident = next((member for member in residents if member.email == email), None)
        if email and not resident:
            abort(400, description="Select an active resident of this organisation.")
        member, property_record = register.intake_links(
            org.id, current_user.id, request.form.get("member_id"), request.form.get("property_id"))
        if resident and member and member.membership.user_id != resident.id:
            abort(400, description="The selected member and resident account do not match.")
        title = (request.form.get("title") or "").strip()
        description = (request.form.get("description") or "").strip()
        category = request.form.get("category")
        channel = request.form.get("channel")
        priority = request.form.get("priority")
        if (not title or len(title) > 255 or not description
                or category not in CATEGORIES or channel not in CHANNELS
                or priority not in PRIORITIES):
            abort(400, description="Complete the required issue fields with valid values.")
        for attempt in range(5):
            ref = f"{org.slug[:2].upper()}-{random.randint(10000, 99999)}"
            ix = CoreInteraction(
                reference=ref, organization_id=org.id,
                creator_id=resident.id if resident else current_user.id,
                title=title, description=description, channel=channel,
                category=category, interaction_type=category, priority=priority, status="NEW",
                member_id=member.id if member else None,
                property_id=property_record.id if property_record else None,
                recorded_by=current_user.id
            )
            db.session.add(ix)
            try:
                audit.record(org.id, current_user.id, "interaction.created", ix)
                sla.intake(ix)
                db.session.commit()
            except IntegrityError:
                db.session.rollback()
                # Retry only reference collisions; do not conceal other database errors.
                if not CoreInteraction.query.filter_by(reference=ref).first():
                    raise
            else:
                flash(f"Interaction {ref} logged successfully.", "success")
                return redirect(url_for("uip_bp.view_interaction", org_slug=org.slug, reference=ix.reference))
        flash("Could not allocate an issue reference. Please try again.", "warning")
        return render_template("uip/reception/new_interaction.html", org=org, residents=residents, register_members=register_members, register_properties=register_properties), 409
    return render_template("uip/reception/new_interaction.html", org=org, residents=residents, register_members=register_members, register_properties=register_properties)


@uip_bp.route("/<org_slug>/interaction/<reference>")
@login_required
def view_interaction(org_slug, reference):
    role = _require_role("manager", "receptionist", "committee_member", "resident")
    ix = _interaction(reference)
    if role == "resident" and ix.creator_id != current_user.id:
        abort(403)
    return render_template("uip/interactions/view.html", org=g.organization,
                           interaction=ix, staff=_members_with_roles(*STAFF_ROLES).all(),
                           current_role=role, task_actionable=operations.actionable, request_key=str(uuid.uuid4()),
                           eligible_providers=[r["provider"] for r in routing.recommend(g.organization.id, current_user.id, ix.id)] if role in STAFF_ROLES else [],
                           register_member=UipMemberProfile.query.filter_by(id=ix.member_id, organization_id=g.organization.id).first() if ix.member_id else None,
                           register_property=UipProperty.query.filter_by(id=ix.property_id, organization_id=g.organization.id).first() if ix.property_id else None)


@uip_bp.route("/<org_slug>/interaction/<reference>/task", methods=["POST"])
@login_required
def add_task(org_slug, reference):
    _require_role("manager", "receptionist")
    ix = _interaction(reference)
    title = (request.form.get("title") or "").strip()
    if not title or len(title) > 255:
        abort(400, description="A valid task title is required.")
    assignee_id = request.form.get("assignee_id")
    if assignee_id:
        assignee_id = _positive_id(assignee_id)
        if not _members_with_roles(*STAFF_ROLES).filter(User.id == assignee_id).first():
            abort(400, description="Select active staff from this organisation.")
    task = operations.add_task(g.organization.id, current_user.id, ix.id, title,
                               request.form.get("description"), assignee_id or None)
    db.session.commit()
    flash("Task added successfully.", "success")
    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=reference))


@uip_bp.route("/<org_slug>/task/<int:task_id>/complete", methods=["POST"])
@login_required
def complete_task(org_slug, task_id):
    task = operations.finish_task(g.organization.id, current_user.id, task_id,
                                  expected=request.form.get("expected_version"))
    db.session.commit()
    flash("Task marked as completed.", "success")
    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=task.interaction.reference))


@uip_bp.route("/<org_slug>/interaction/<reference>/resolve", methods=["POST"])
@login_required
def resolve_interaction(org_slug, reference):
    _require_role("manager", "receptionist")
    ix = _interaction(reference)
    operations.resolve(g.organization.id, current_user.id, ix.id)
    db.session.commit()
    flash("Interaction resolved successfully.", "success")
    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=reference))


@uip_bp.route("/<org_slug>/interaction/<reference>/summarize", methods=["POST"])
@login_required
def summarize_interaction(org_slug, reference):
    _require_role("manager", "receptionist")
    ix = _interaction(reference)
    result = LunaGateway.ask_luna("", interaction_id=ix.id)
    flash(result["message"], "warning")
    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=reference))


@uip_bp.route("/<org_slug>/interaction/<reference>/provider", methods=["POST"])
@login_required
def assign_provider(org_slug, reference):
    _require_role(*STAFF_ROLES)
    ix = _interaction(reference)
    provider_id = _positive_id(request.form.get("provider_id"))
    if not UipProvider.query.filter_by(id=provider_id, organization_id=g.organization.id).first():
        abort(400, description="Select a provider from this organisation.")
    order = work_orders.create(g.organization.id, current_user.id, ix.id, provider_id,
        request.form.get("description"), request.form.get("service_location"), request.form.get("request_key"))
    db.session.commit()
    flash("Work order created. Dispatch has not yet been recorded.", "success")
    return redirect(url_for("uip_bp.work_order_view", org_slug=org_slug, order_id=order.id))


@uip_bp.route("/<org_slug>/interaction/<reference>/municipal", methods=["POST"])
@login_required
def escalate_municipality(org_slug, reference):
    _require_role("manager", "receptionist", "committee_member")
    ix = _interaction(reference)
    department = (request.form.get("department") or "").strip()
    mun_ref = (request.form.get("municipality_reference") or "").strip()
    if not department or len(department) > 100 or len(mun_ref) > 100:
        abort(400, description="A valid department and reference are required.")
    referral = reception.referral(g.organization.id, current_user.id, ix.id, department, reference=mun_ref)
    if ix.status == "NEW":
        ix.status = "IN_PROGRESS"
    db.session.commit()
    flash(f"Referral recorded ({department}).", "info")
    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=reference))


@uip_bp.route("/<org_slug>/reports")
@login_required
def org_reports(org_slug):
    role = _require_role("manager", "committee_member")
    from app.uip.services.governance import overview
    report = metrics(g.organization.id, current_user.id) if role == "manager" else overview(g.organization.id, current_user.id)
    return render_template("uip/dashboards/reports.html", org=g.organization, report=report, report_role=role)


@uip_bp.route("/<org_slug>/reports/generate_ai", methods=["POST"])
@login_required
def generate_ai_report(org_slug):
    _require_role("manager", "committee_member")
    result = LunaGateway.ask_luna("")
    flash(result["message"], "warning")
    return redirect(url_for("uip_bp.org_reports", org_slug=org_slug))


@uip_bp.route("/")
def uip_start():
    return render_template('uip/public_about.html')

@uip_bp.route("/price")
def price_page():
    from app.models.auth import AuthSubject
    from app.enrollment.logic import get_quote_for_subject_country
    from flask import session

    subject = AuthSubject.query.filter(
        db.func.lower(AuthSubject.slug) == 'uip').first()
    if not subject:
        flash("Subject not found.", "warning")
        return redirect(url_for('public_bp.welcome'))

    country_code = (request.args.get("country") or "").strip().upper()
    if not country_code:
        country_code = 'ZA'  # Default to SA

    quote = get_quote_for_subject_country(subject.id, country_code)
    session["country_code"] = country_code

    return render_template(
        "uip/price.html",
        subject=subject,
        country_code=country_code,
        quote=quote
    )


@uip_bp.errorhandler(IntegrityError)
def register_conflict(error):
    db.session.rollback()
    return render_template("uip/validation_error.html", org=getattr(g, "organization", None),
        error="The record conflicts with an existing reference or relationship. Review the values and retry."), 409


def _register_context():
    return dict(org=g.organization,
                can_manage=CoreRoleAssignment.query.filter(
                    CoreRoleAssignment.organization_id == g.organization.id,
                    CoreRoleAssignment.user_id == current_user.id,
                    CoreRoleAssignment.role.has(db.and_(CoreRole.slug.in_(audit.WRITE_ROLES),
                        db.or_(CoreRole.organization_id.is_(None), CoreRole.organization_id == g.organization.id)))
                ).first() is not None)


@uip_bp.route("/<org_slug>/members")
@login_required
def member_list(org_slug):
    query = register.members(g.organization.id, current_user.id)
    search = request.args.get("q", "").strip()[:120]
    status = request.args.get("status", "all")
    if search:
        query = query.filter(db.or_(UipMemberProfile.name.ilike(f"%{search}%"), UipMemberProfile.reference.ilike(f"%{search}%"), UipMemberProfile.email.ilike(f"%{search}%"), UipMemberProfile.phone.ilike(f"%{search}%")))
    if status in {"active", "inactive"}:
        query = query.filter_by(is_active=status == "active")
    page = query.paginate(
        page=request.args.get("page", 1, type=int), per_page=30, error_out=False)
    from app.uip.presentation import register_links, current_relationship
    members, properties, links = register_links(g.organization.id)
    linked = {m.id: [properties[l.property_id] for l in links if l.member_id == m.id and current_relationship(l)] for m in page.items}
    return render_template("uip/members/list.html", page=page, linked=linked, search=search, status=status, **_register_context())


@uip_bp.route("/<org_slug>/members/new", methods=["GET", "POST"])
@uip_bp.route("/<org_slug>/members/<int:member_id>/edit", methods=["GET", "POST"])
@login_required
def member_form(org_slug, member_id=None):
    audit.authorize(g.organization.id, current_user.id, audit.WRITE_ROLES)
    member = register.get(UipMemberProfile, g.organization.id, current_user.id, member_id) if member_id else None
    if request.method == "POST":
        member = register.save_member(g.organization.id, current_user.id, request.form, member_id)
        db.session.commit()
        flash("Member register saved. Application access and roles are unchanged.", "success")
        if not member_id and request.args.get("return_to") == "intake":
            return redirect(url_for("uip_bp.new_interaction", org_slug=org_slug, member_id=member.id,
                                    property_id=request.args.get("property_id", type=int)))
        return redirect(url_for("uip_bp.member_view", org_slug=org_slug, member_id=member.id))
    return render_template("uip/members/form.html", member=member,
        account_names={u.id: u.name or u.email or str(u.id) for u in User.query.join(
            CoreOrganizationMember, CoreOrganizationMember.user_id == User.id).filter(
                CoreOrganizationMember.organization_id == g.organization.id,
                CoreOrganizationMember.is_active.is_(True)).all()},
        memberships=register.available_memberships(g.organization.id, current_user.id), **_register_context())


@uip_bp.route("/<org_slug>/members/<int:member_id>")
@login_required
def member_view(org_slug, member_id):
    member = register.get(UipMemberProfile, g.organization.id, current_user.id, member_id)
    return render_template("uip/members/view.html", member=member,
        members=register.members(g.organization.id, current_user.id).all(),
        ownerships=UipPropertyMember.query.filter_by(organization_id=g.organization.id, member_id=member.id).all(),
        representations=UipMemberRepresentative.query.filter_by(organization_id=g.organization.id, member_id=member.id).all(),
        preferences=UipCommunicationPreference.query.filter_by(organization_id=g.organization.id, member_id=member.id).all(),
        properties=register.properties(g.organization.id, current_user.id, active_only=True).all(),
        prior_issues=CoreInteraction.query.filter_by(organization_id=g.organization.id, member_id=member.id).order_by(CoreInteraction.id.desc()).all(),
        channels=register.CHANNELS, **_register_context())


@uip_bp.route("/<org_slug>/members/<int:member_id>/representatives", methods=["POST"])
@uip_bp.route("/<org_slug>/members/<int:member_id>/representatives/<int:link_id>", methods=["POST"])
@login_required
def member_representative(org_slug, member_id, link_id=None):
    register.save_relationship(g.organization.id, current_user.id, request.form, member_id=member_id, link_id=link_id)
    db.session.commit()
    flash("Representation recorded; no voting or governance rights were granted.", "success")
    return redirect(url_for("uip_bp.member_view", org_slug=org_slug, member_id=member_id))


@uip_bp.route("/<org_slug>/members/<int:member_id>/preferences", methods=["POST"])
@login_required
def member_preference(org_slug, member_id):
    register.set_preference(g.organization.id, current_user.id, member_id, request.form)
    db.session.commit()
    flash("Communication preference recorded. No message was sent.", "success")
    return redirect(url_for("uip_bp.member_view", org_slug=org_slug, member_id=member_id))


@uip_bp.route("/<org_slug>/properties")
@login_required
def property_list(org_slug):
    query = register.properties(g.organization.id, current_user.id)
    search = request.args.get("q", "").strip()[:120]
    status = request.args.get("status", "all")
    if search:
        query = query.filter(db.or_(UipProperty.address.ilike(f"%{search}%"), UipProperty.reference.ilike(f"%{search}%"), UipProperty.rates_reference.ilike(f"%{search}%")))
    if status in {"active", "inactive"}:
        query = query.filter_by(is_active=status == "active")
    page = query.paginate(
        page=request.args.get("page", 1, type=int), per_page=30, error_out=False)
    from app.uip.presentation import register_links, current_relationship
    members, properties, links = register_links(g.organization.id)
    linked = {p.id: [members[l.member_id] for l in links if l.property_id == p.id and current_relationship(l)] for p in page.items}
    return render_template("uip/properties/list.html", page=page, linked=linked, search=search, status=status, **_register_context())


@uip_bp.route("/<org_slug>/properties/new", methods=["GET", "POST"])
@uip_bp.route("/<org_slug>/properties/<int:property_id>/edit", methods=["GET", "POST"])
@login_required
def property_form(org_slug, property_id=None):
    audit.authorize(g.organization.id, current_user.id, audit.WRITE_ROLES)
    item = register.get(UipProperty, g.organization.id, current_user.id, property_id) if property_id else None
    if request.method == "POST":
        item = register.save_property(g.organization.id, current_user.id, request.form, property_id)
        db.session.commit()
        flash("Property saved.", "success")
        if not property_id and request.args.get("return_to") == "intake":
            return redirect(url_for("uip_bp.new_interaction", org_slug=org_slug, property_id=item.id,
                                    member_id=request.args.get("member_id", type=int)))
        return redirect(url_for("uip_bp.property_view", org_slug=org_slug, property_id=item.id,
                                member_id=request.args.get("member_id", type=int)))
    return render_template("uip/properties/form.html", item=item, **_register_context())


@uip_bp.route("/<org_slug>/properties/<int:property_id>")
@login_required
def property_view(org_slug, property_id):
    item = register.get(UipProperty, g.organization.id, current_user.id, property_id)
    return render_template("uip/properties/view.html", item=item,
        members=register.members(g.organization.id, current_user.id).all(),
        ownerships=UipPropertyMember.query.filter_by(organization_id=g.organization.id, property_id=item.id).all(),
        prior_issues=CoreInteraction.query.filter_by(organization_id=g.organization.id, property_id=item.id).order_by(CoreInteraction.id.desc()).all(),
        **_register_context())


@uip_bp.route("/<org_slug>/properties/<int:property_id>/members", methods=["POST"])
@uip_bp.route("/<org_slug>/properties/<int:property_id>/members/<int:link_id>", methods=["POST"])
@login_required
def property_member(org_slug, property_id, link_id=None):
    register.save_relationship(g.organization.id, current_user.id, request.form, property_id=property_id, link_id=link_id)
    db.session.commit()
    flash("Property relationship recorded; eligibility is maintained separately.", "success")
    return redirect(url_for("uip_bp.property_view", org_slug=org_slug, property_id=property_id))


@uip_bp.route("/<org_slug>/audit")
@login_required
def audit_history(org_slug):
    page = audit.events(g.organization.id, current_user.id).paginate(
        page=request.args.get("page", 1, type=int), per_page=30, error_out=False)
    return render_template("uip/audit/list.html", org=g.organization, page=page, event=None)


@uip_bp.route("/<org_slug>/audit/<int:event_id>")
@login_required
def audit_event(org_slug, event_id):
    event = audit.events(g.organization.id, current_user.id, event_id=event_id)
    return render_template("uip/audit/list.html", org=g.organization, page=None, event=event)


@uip_bp.route("/<org_slug>/task/<int:task_id>/cancel", methods=["POST"])
@login_required
def cancel_task(org_slug, task_id):
    task = operations.finish_task(g.organization.id, current_user.id, task_id, cancel=True,
        reason=request.form.get("reason"), expected=request.form.get("expected_version"))
    db.session.commit()
    flash("Internal task cancelled; its history is retained.", "success")
    return redirect(url_for("uip_bp.view_interaction", org_slug=org_slug, reference=task.interaction.reference))


@uip_bp.route("/<org_slug>/providers")
@login_required
def provider_list(org_slug):
    _require_role("manager")
    rows = UipProvider.query.filter_by(organization_id=g.organization.id).order_by(UipProvider.name).all()
    from app.models.uip import UipProviderCapability
    caps = UipProviderCapability.query.filter_by(organization_id=g.organization.id).all()
    active_work = dict(db.session.query(UipWorkOrder.provider_id, db.func.count(UipWorkOrder.id)).filter(
        UipWorkOrder.organization_id == g.organization.id, UipWorkOrder.status.notin_(providers.TERMINAL)).group_by(UipWorkOrder.provider_id).all())
    return render_template("uip/providers/list.html", org=g.organization, providers=rows,
        services={p.id: [c.category for c in caps if c.provider_id == p.id] for p in rows}, active_work=active_work)


@uip_bp.route("/<org_slug>/providers/new", methods=["GET", "POST"])
@uip_bp.route("/<org_slug>/providers/<int:provider_id>/edit", methods=["GET", "POST"])
@login_required
def provider_form(org_slug, provider_id=None):
    _require_role("manager")
    provider = providers.get(g.organization.id, provider_id) if provider_id else None
    if request.method == "POST":
        provider = providers.save(g.organization.id, current_user.id, request.form,
            request.form.getlist("capabilities"), provider_id, request.form.get("expected_version"))
        db.session.commit()
        flash("Provider saved.", "success")
        return redirect(url_for("uip_bp.provider_view", org_slug=org_slug, provider_id=provider.id))
    from app.models.uip import UipProviderCapability
    selected = {c.category for c in UipProviderCapability.query.filter_by(organization_id=g.organization.id, provider_id=provider_id)} if provider else set()
    return render_template("uip/providers/form.html", org=g.organization, provider=provider,
                           categories=sorted(CATEGORIES), selected=selected)


@uip_bp.route("/<org_slug>/providers/<int:provider_id>")
@login_required
def provider_view(org_slug, provider_id):
    _require_role("manager")
    from app.models.uip import UipProviderCapability, UipProviderUser
    provider = providers.get(g.organization.id, provider_id)
    links = db.session.query(UipProviderUser, User).join(CoreOrganizationMember,
        CoreOrganizationMember.id == UipProviderUser.membership_id).join(User, User.id == CoreOrganizationMember.user_id).filter(
        UipProviderUser.organization_id == g.organization.id, UipProviderUser.provider_id == provider_id,
        CoreOrganizationMember.organization_id == g.organization.id).all()
    members = db.session.query(CoreOrganizationMember, User).join(User, User.id == CoreOrganizationMember.user_id).filter(
        CoreOrganizationMember.organization_id == g.organization.id, CoreOrganizationMember.is_active.is_(True),
        User.id.in_([u.id for u in _members_with_roles("provider").all()])).all()
    capabilities = UipProviderCapability.query.filter_by(organization_id=g.organization.id, provider_id=provider_id).all()
    return render_template("uip/providers/view.html", org=g.organization, provider=provider, links=links, members=members, capabilities=capabilities)


@uip_bp.route("/<org_slug>/providers/<int:provider_id>/deactivate", methods=["POST"])
@login_required
def provider_deactivate(org_slug, provider_id):
    providers.deactivate(g.organization.id, current_user.id, provider_id, request.form.get("expected_version"))
    db.session.commit()
    flash("Provider deactivated.", "success")
    return redirect(url_for("uip_bp.provider_view", org_slug=org_slug, provider_id=provider_id))


@uip_bp.route("/<org_slug>/providers/<int:provider_id>/users", methods=["POST"])
@login_required
def provider_link(org_slug, provider_id):
    providers.associate(g.organization.id, current_user.id, provider_id,
        _positive_id(request.form.get("membership_id")), request.form.get("expected_version"))
    db.session.commit()
    flash("Provider association recorded. No role was granted.", "success")
    return redirect(url_for("uip_bp.provider_view", org_slug=org_slug, provider_id=provider_id))


@uip_bp.route("/<org_slug>/providers/<int:provider_id>/users/<int:link_id>/revoke", methods=["POST"])
@login_required
def provider_revoke(org_slug, provider_id, link_id):
    providers.revoke(g.organization.id, current_user.id, provider_id, link_id, request.form.get("expected_version"))
    db.session.commit()
    flash("Provider association revoked.", "success")
    return redirect(url_for("uip_bp.provider_view", org_slug=org_slug, provider_id=provider_id))


@uip_bp.route("/<org_slug>/work-orders")
@login_required
def work_order_list(org_slug):
    rows = work_orders.orders(g.organization.id, current_user.id).limit(200).all()
    return render_template("uip/work_orders/list.html", org=g.organization,
        orders=[work_orders.projection(g.organization.id, current_user.id, row.id) for row in rows])


@uip_bp.route("/<org_slug>/work-orders/<int:order_id>")
@login_required
def work_order_view(org_slug, order_id):
    from app.models.uip import UipWorkOrderAction
    order = work_orders.get(g.organization.id, current_user.id, order_id)
    staff_access = work_orders.is_staff(g.organization.id, current_user.id)
    allowed = []
    for action, (sources, destination, roles, reason_required) in work_orders.TRANSITIONS.items():
        if order.status not in sources:
            continue
        from werkzeug.exceptions import Forbidden
        try:
            audit.authorize(g.organization.id, current_user.id, roles)
        except Forbidden:
            continue
        if roles == ("provider",) and not providers.linked(g.organization.id, order.provider_id, current_user.id):
            continue
        if action in {"verified", "verification_rejected", "closed"} and providers.associated(g.organization.id, order.provider_id, current_user.id):
            continue
        allowed.append((action, reason_required))
    journal = UipWorkOrderAction.query.filter_by(organization_id=g.organization.id, work_order_id=order.id).order_by(UipWorkOrderAction.resulting_version).all()
    history = [dict(action=r.action, previous_state=r.previous_state, new_state=r.new_state,
                    occurred_at=r.occurred_at, actor_user_id=r.actor_user_id if staff_access else None,
                    version=r.resulting_version, reason_code=r.reason_code, dispatch_method=r.dispatch_method, note=r.note if staff_access or r.actor_user_id == current_user.id or r.action in {"verification_rejected", "cancelled"} else None)
               for r in journal if staff_access or r.action != "created"]
    return render_template("uip/work_orders/view.html" if staff_access else "uip/work_orders/provider_view.html",
        org=g.organization, order=work_orders.projection(g.organization.id, current_user.id, order_id),
        issue_reference=order.interaction.reference if staff_access else None, history=history,
        actions=allowed, request_key=str(uuid.uuid4()), reason_codes=sorted(audit.REASON_CODES),
        dispatch_methods=sorted(audit.DISPATCH_METHODS))


@uip_bp.route("/<org_slug>/work-orders/<int:order_id>/actions", methods=["POST"])
@login_required
def work_order_action(org_slug, order_id):
    order = work_orders.transition(g.organization.id, current_user.id, order_id, request.form.get("action"),
        request.form.get("expected_version"), request.form.get("request_key"), request.form.get("note"),
        request.form.get("reason_code"), request.form.get("dispatch_method"))
    db.session.commit()
    flash("Work-order action recorded.", "success")
    return redirect(url_for("uip_bp.work_order_view", org_slug=org_slug, order_id=order.id))
