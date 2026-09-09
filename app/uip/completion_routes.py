"""Visible UIP registers, setup and bounded CSV capture; no schema/startup writes."""
import csv
import hashlib
import io

from flask import abort, g, request, render_template, url_for, flash, redirect, current_app
from flask_login import current_user, login_required
from itsdangerous import URLSafeTimedSerializer, BadSignature
from werkzeug.exceptions import HTTPException
from sqlalchemy.exc import IntegrityError

from app.extensions import db
from app.models.core import CoreInteraction, CoreTask, CoreRoleAssignment, CoreRole
from app.models.uip import UipMemberProfile, UipProperty, UipMunicipalReferral, UipCommunicationLog
from app.uip import uip_bp
from app.uip.services import audit, register, providers, operations
from app.uip.operational_routes import page, link


@uip_bp.context_processor
def navigation_context():
    if not current_user.is_authenticated or not getattr(g, "organization", None):
        return dict(uip_navigation=[], uip_nav_groups={}, uip_roles=set(), uip_can_capture=False, uip_can_log=False)
    roles = {a.role.slug for a in CoreRoleAssignment.query.filter(
        CoreRoleAssignment.organization_id == g.organization.id,
        CoreRoleAssignment.user_id == current_user.id,
        CoreRoleAssignment.role.has(db.or_(CoreRole.organization_id.is_(None),
                                          CoreRole.organization_id == g.organization.id))).all()}
    staff = set(providers.STAFF)
    admin = {"manager", "committee_member"}
    entries = (
        ("Command Centre", "dashboard", roles),
        ("Getting started", "setup_page", set(audit.WRITE_ROLES)),
        ("Members / Ratepayers", "member_list", set(audit.READ_ROLES)),
        ("Properties", "property_list", set(audit.READ_ROLES)),
        ("Log interaction", "new_interaction", staff | {"committee_member"}),
        ("Interactions / Issues", "reception_page", staff),
        ("Internal tasks / Follow-ups", "tasks_page", staff),
        ("Providers", "provider_list", {"manager"}), ("Work orders", "work_order_list", staff | {"provider"}),
        ("Routing", "routing_page", {"manager"}), ("SLA policies / Monitoring", "sla_page", staff),
        ("Municipal referrals", "municipal_list", staff | {"committee_member"}),
        ("Communications", "communications_list", staff),
        ("Documents", "documents_page", staff | admin | {"resident", "owner"}),
        ("Meetings / Attendance / Quorum", "meetings_page", admin),
        ("Surveys / Polls", "surveys_page", admin | {"resident", "owner"}),
        ("Governance decisions", "decisions_page", admin),
        *((label, endpoint, {"manager", "committee_member", "owner", "resident"}) for label, endpoint in (("Finance overview", "finance_overview"), ("Transactions", "finance_transactions"), ("Budget", "finance_budget"), ("Commitments", "finance_commitments"))),
        ("Reports / Exports", "org_reports", admin),
        ("Organisation settings", "org_settings", set(audit.WRITE_ROLES)),
        ("UIP audit", "audit_history", set(audit.AUDIT_ROLES)),
        ("AI & Wallet", "ai_wallet", {"manager"}),
    )
    sections = (
        ("Command Centre", [("Overview", "dashboard")]),
        ("Residents & Properties", [("Ratepayers", "member_list"), ("Properties", "property_list"), ("Import Register", "register_import")]),
        ("Operations", [("Interactions & Issues", "reception_page"), ("Tasks / Follow-ups", "tasks_page"), ("Municipal Matters", "municipal_list"), ("Communications", "communications_list")]),
        ("Service Providers", [("Providers", "provider_list"), ("Work Orders", "work_order_list"), ("Routing & SLA", "service_standards")]),
        ("Governance", [("Meetings", "meetings_page"), ("Surveys", "surveys_page"), ("Decisions", "decisions_page"), ("Documents", "documents_page")]),
        ("Finance", [("Overview", "finance_overview"), ("Transactions", "finance_transactions"), ("Budget", "finance_budget"), ("Commitments", "finance_commitments")]),
        ("Reports", [("Reports / Exports", "org_reports")]),
        ("Administration", [("Organisation Settings", "org_settings"), ("AI & Wallet", "ai_wallet"), ("UIP Audit", "audit_history")]),
    )
    allowed = {target for label, target, permitted in entries if roles & permitted}
    if roles & set(audit.WRITE_ROLES):
        allowed.add("register_import")
    if roles & staff:
        allowed.add("service_standards")
    if roles == {"provider"}:
        allowed.discard("dashboard")
    aliases = {"voting_invitations": "surveys_page", "ai_assistant": "ai_wallet", "finance_transaction_new": "finance_transactions", "finance_transaction": "finance_transactions", "finance_commitment": "finance_commitments", "finance_report": "finance_overview", "setup_page": "org_settings","member_form": "member_list", "member_view": "member_list", "property_form": "property_list", "property_view": "property_list",
        "new_interaction": "reception_page", "view_interaction": "reception_page", "reception_issue": "reception_page",
        "provider_form": "provider_list", "provider_view": "provider_list", "work_order_view": "work_order_list",
        "routing_page": "service_standards", "provider_performance": "service_standards", "sla_page": "service_standards", "referral_page": "municipal_list",
        "meeting_page": "meetings_page", "survey_page": "surveys_page", "document_page": "documents_page", "audit_event": "audit_history"}
    endpoint = (request.endpoint or "").split(".")[-1]
    active = aliases.get(endpoint, endpoint)
    groups = {group: [dict(link(label, target), active=active == target) for label, target in items if target in allowed] for group, items in sections}
    groups = {group: items for group, items in groups.items() if items}
    from app.uip.presentation import current_relationship, display_value
    return dict(uip_navigation=[item for items in groups.values() for item in items], uip_nav_groups=groups, uip_is_current=current_relationship, uip_display=display_value,
                uip_can_capture=bool(roles & set(audit.WRITE_ROLES)), uip_roles=roles,
                uip_can_log=bool(roles & (staff | {"committee_member"})))


@uip_bp.route("/<org_slug>/getting-started")
@login_required
def setup_page(org_slug):
    audit.authorize(g.organization.id, current_user.id, audit.WRITE_ROLES)
    return render_template("uip/setup.html", org=g.organization)


@uip_bp.route("/<org_slug>/operations/tasks")
@login_required
def tasks_page(org_slug):
    org = g.organization.id
    audit.authorize(org, current_user.id, providers.STAFF)
    rows = db.session.query(CoreTask, CoreInteraction).join(CoreInteraction,
        CoreTask.interaction_id == CoreInteraction.id).filter(CoreInteraction.organization_id == org).all()
    return page("Internal tasks / Follow-ups", ["Task", "Issue", "Status", "Due", "Actions"],
        [(t.title, ix.reference, t.status, t.due_date, link("Manage task and issue", "view_interaction", reference=ix.reference)) for t, ix in rows],
        notes=[link("Record or complete structured follow-ups", "reception_page"),
               link("Log an interaction to create an internal task", "new_interaction")])


@uip_bp.route("/<org_slug>/operations/municipal")
@login_required
def municipal_list(org_slug):
    audit.authorize(g.organization.id, current_user.id, ("manager", "receptionist", "committee_member"))
    rows = UipMunicipalReferral.query.filter_by(organization_id=g.organization.id).all()
    return page("Municipal Matters", ["Department", "Municipal reference", "Status", "Issue"],
        [(link(r.department, "referral_page", referral_id=r.id), r.municipality_reference, r.status,
          link("Related issue", "view_interaction", reference=CoreInteraction.query.filter_by(
              organization_id=g.organization.id, id=r.interaction_id).one().reference)) for r in rows],
        notes=["Record referrals from the related issue. A recorded referral is not proof of submission or delivery."])


@uip_bp.route("/<org_slug>/operations/communications")
@login_required
def communications_list(org_slug):
    audit.authorize(g.organization.id, current_user.id, providers.STAFF)
    rows = UipCommunicationLog.query.filter_by(organization_id=g.organization.id).order_by(UipCommunicationLog.id.desc()).all()
    return page("Communications", ["Channel", "Direction", "Purpose", "Recorded status", "Issue"],
        [(r.channel, r.direction, r.purpose, r.status,
          link("View activity / record communication", "reception_issue", issue_id=r.interaction_id) if r.interaction_id else "No issue linked") for r in rows],
        notes=[link("Choose an issue to record communication", "reception_page"),
               "Recording communication does not send it. No delivery is implied."])


@uip_bp.route("/<org_slug>/operations/routing")
@login_required
def routing_page(org_slug):
    from app.models.uip import UipProvider
    audit.authorize(g.organization.id, current_user.id, ("manager",))
    rows = UipProvider.query.filter_by(organization_id=g.organization.id).all()
    return page("Provider routing", ["Provider", "Availability", "Active"],
        [(link(p.name, "provider_view", provider_id=p.id), p.availability, p.is_active) for p in rows],
        notes=["Configure category capabilities, availability and provider-user associations in each provider record. Eligible providers are selected from the related issue; suggestions are ordered by current workload.",
               link("Provider register", "provider_list"), link("Choose issue for work-order routing", "reception_page")])


@uip_bp.route("/<org_slug>/service-standards")
@login_required
def service_standards(org_slug):
    audit.authorize(g.organization.id, current_user.id, providers.STAFF)
    return render_template("uip/service_standards.html", org=g.organization)


CSV_COLUMNS = {
    "members": ("reference", "name", "member_type", "email", "phone", "is_active", "eligibility_status"),
    "properties": ("reference", "address", "rates_reference", "classification", "is_active"),
    "relationships": ("member_reference", "property_reference", "relationship", "valid_from", "valid_to", "is_verified"),
}


@uip_bp.errorhandler(400)
@uip_bp.errorhandler(409)
def operational_validation(error):
    db.session.rollback()
    return render_template("uip/validation_error.html", org=getattr(g, "organization", None),
                           error=error.description), error.code


def import_rows(kind, content):
    """All validation uses the same scoped, audited manual-capture services."""
    if kind not in CSV_COLUMNS:
        abort(400, description="Choose members, properties or relationships.")
    try:
        reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")), strict=True)
        if not reader.fieldnames or len(reader.fieldnames) != len(set(reader.fieldnames)) or set(reader.fieldnames) != set(CSV_COLUMNS[kind]):
            abort(400, description="Use the exact displayed CSV columns, without duplicates.")
        rows = list(reader)
    except (UnicodeError, csv.Error):
        abort(400, description="Upload a valid UTF-8 CSV file.")
    if not 1 <= len(rows) <= 200:
        abort(400, description="Import between 1 and 200 rows at a time.")
    org, actor = g.organization.id, current_user.id
    preview = []
    for number, row in enumerate(rows, 2):
        try:
            if None in row or any(v is None for v in row.values()):
                abort(400, description="Column count does not match the header.")
            row = {k: v.strip() for k, v in row.items()}
            if kind == "members":
                if UipMemberProfile.query.filter_by(organization_id=org, reference=row["reference"]).first():
                    abort(409, description="Duplicate member reference. Existing records are never overwritten.")
                if row["email"] and UipMemberProfile.query.filter(UipMemberProfile.organization_id == org,
                    db.func.lower(UipMemberProfile.email) == row["email"].lower()).first():
                    abort(409, description="Contact email already registered; review possible duplicate manually.")
                register.save_member(org, actor, row)
            elif kind == "properties":
                if UipProperty.query.filter_by(organization_id=org, reference=row["reference"]).first():
                    abort(409, description="Duplicate property reference. Existing records are never overwritten.")
                if UipProperty.query.filter(UipProperty.organization_id == org,
                    db.func.lower(UipProperty.address) == row["address"].lower()).first():
                    abort(409, description="Address already registered; review possible duplicate manually.")
                register.save_property(org, actor, row)
            else:
                from app.models.uip import UipPropertyMember
                member = UipMemberProfile.query.filter_by(organization_id=org, reference=row["member_reference"]).first()
                prop = UipProperty.query.filter_by(organization_id=org, reference=row["property_reference"]).first()
                if not member or not prop:
                    abort(400, description="Member and property references must already exist in this organisation.")
                start, end = register.dates(row)
                if UipPropertyMember.query.filter_by(organization_id=org, member_id=member.id, property_id=prop.id,
                    relationship=row["relationship"], valid_from=start).first():
                    abort(409, description="Duplicate dated relationship.")
                register.save_relationship(org, actor, dict(row, member_id=member.id), property_id=prop.id)
            db.session.flush()
        except register.InvalidRegisterOption as error:
            # Only echo the rejected option, never the rest of the contact row.
            # ASCII escaping exposes invisible characters; Jinja escapes HTML.
            value = error.value
            shown = ascii(value[:80]) + ("... (truncated)" if len(value) > 80 else "") if isinstance(value, str) else ascii(value)
            allowed = ", ".join(ascii(option) for option in error.allowed)
            abort(400, description=f"CSV row {number}: column '{error.column}' has invalid value {shown}. Accepted values: {allowed} (case-sensitive).")
        except HTTPException as error:
            abort(error.code, description=f"CSV row {number}: {error.description}")
        except IntegrityError:
            abort(409, description=f"CSV row {number}: duplicate or conflicting record. Nothing imported.")
        preview.append(row)
    return preview


@uip_bp.route("/<org_slug>/register/import", methods=["GET", "POST"])
@login_required
def register_import(org_slug):
    audit.authorize(g.organization.id, current_user.id, audit.WRITE_ROLES)
    kind = request.form.get("kind", "members")
    rows, token, error = [], None, None
    if request.method == "POST":
        upload = request.files.get("file")
        content = upload.read(256 * 1024 + 1) if upload else b""
        if not content or len(content) > 256 * 1024:
            abort(400, description="Choose a CSV file up to 256 KB.")
        identity = [g.organization.id, current_user.id, kind, hashlib.sha256(content).hexdigest()]
        signer = URLSafeTimedSerializer(current_app.secret_key, salt="uip-register-preview")
        commit = request.form.get("operation") == "commit"
        if commit:
            try:
                if signer.loads(request.form.get("preview_token", ""), max_age=1800) != identity:
                    abort(400, description="Upload the same file and import type that you previewed.")
            except BadSignature:
                abort(400, description="Preview expired or invalid. Preview the file again.")
        try:
            transaction = db.session.begin_nested()
            rows = import_rows(kind, content)
            if commit:
                transaction.commit()
                db.session.commit()
                flash(f"Imported {len(rows)} {kind}. No accounts or messages were created.", "success")
                return redirect(url_for("uip_bp.register_import", org_slug=org_slug))
            transaction.rollback()
            token = signer.dumps(identity)
        except HTTPException as exc:
            db.session.rollback()
            rows, error = [], exc.description
    return render_template("uip/register_import.html", org=g.organization, columns=CSV_COLUMNS,
        kind=kind, rows=rows, preview_token=token, error=error), (400 if error else 200)
