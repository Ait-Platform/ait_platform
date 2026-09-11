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
        ("Committee Dashboard", "committee_dashboard", {"manager", "committee_member"}),
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
        ("Governance", [("Committee Dashboard", "committee_dashboard"), ("Meetings", "meetings_page"), ("Surveys", "surveys_page"), ("Decisions", "decisions_page"), ("Documents", "documents_page")]),
        ("Finance", [("Overview", "finance_overview"), ("Transactions", "finance_transactions"), ("Budget", "finance_budget"), ("Commitments", "finance_commitments")]),
        ("Reports", [("Reports / Exports", "org_reports")]),
        ("Administration", [("Organisation Settings", "org_settings"), ("AI & Wallet", "ai_wallet"), ("UIP Audit", "audit_history")]),
    )
    allowed = {target for label, target, permitted in entries if roles & permitted}
    from app.uip.services.register import require_register_admin
    is_register_admin = False
    try:
        require_register_admin(g.organization.id, current_user.id)
        is_register_admin = True
    except Exception:
        pass
        
    if is_register_admin:
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


@uip_bp.route("/<org_slug>/register/import", methods=["GET", "POST"])
@login_required
def register_import(org_slug):
    from app.uip.services.register import require_register_admin, process_import_batch
    from app.models.uip import UipDocument
    from datetime import datetime
    import csv, io
    require_register_admin(g.organization.id, current_user.id)
    kind = request.form.get("kind", "members")
    rows, token, error, summary = [], None, None, None
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
            reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")), strict=True)
            if not reader.fieldnames or len(reader.fieldnames) != len(set(reader.fieldnames)) or set(reader.fieldnames) != set(CSV_COLUMNS[kind]):
                abort(400, description="Use the exact displayed CSV columns, without duplicates.")
            rows = list(reader)
        except (UnicodeError, csv.Error):
            abort(400, description="Upload a valid UTF-8 CSV file.")
            
        if not 1 <= len(rows) <= 200:
            abort(400, description="Import between 1 and 200 rows at a time.")
            
        try:
            transaction = db.session.begin_nested()
            
            # Save Document
            doc = UipDocument(organization_id=g.organization.id, uploader_id=current_user.id, title=f"Import {kind} {datetime.now().strftime('%Y-%m-%d')}", category="MUNICIPAL_REGISTER")
            db.session.add(doc)
            db.session.flush()
            
            metadata = {
                "source_identifier": request.form.get("source_identifier"),
                "batch_reference": request.form.get("batch_reference"),
                "date_received": datetime.utcnow().date(),
                "effective_date": datetime.strptime(request.form.get("effective_date", datetime.utcnow().strftime("%Y-%m-%d")), "%Y-%m-%d").date(),
                "document_id": doc.id
            }
            
            batch, summary = process_import_batch(g.organization.id, current_user.id, kind, rows, metadata)
            
            if commit:
                transaction.commit()
                db.session.commit()
                flash(f"Import {batch.status}. {summary['created']} created, {summary['updated']} updated, {summary['exceptions']} exceptions.", "success")
                return redirect(url_for("uip_bp.register_import", org_slug=org_slug))
            else:
                transaction.rollback()
                token = signer.dumps(identity)
                
        except Exception as err:
            transaction.rollback()
            db.session.rollback()
            error = str(getattr(err, "description", err))
            
    return render_template("uip/register_import.html", org=g.organization, columns=CSV_COLUMNS,
                           kind=kind, rows=rows, preview_token=token, summary=summary, error=error), (400 if error else 200)
