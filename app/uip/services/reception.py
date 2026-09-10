"""Structured reception and municipal history. Caller owns commit/rollback."""
from datetime import datetime, timezone
from flask import abort
from app.extensions import db
from app.models.core import CoreInteraction
from app.models.uip import UipMunicipalReferral, UipWorkOrder
from app.models.uip_operations import UipFollowUp, UipCommunicationLog, UipReferralEvent
from . import audit, operations, providers
from .sla import utc

# Optional intake suggestions; unmapped categories retain free-text titles.
SHORT_TITLE_PRESETS = {
    "CLEANING": {
        "presets": [
            "Illegal dumping / rubble",
            "Littering",
            "Street sweeping required",
            "Overflowing bin",
            "Bin not collected",
            "Public area needs cleaning",
            "Overgrown verge",
            "Blocked drain / debris",
            "Dumping on vacant property",
            "Dead animal removal",
            "Graffiti removal",
        ],
        "other_label": "Other cleaning issue",
    },

    "GENERAL ENQUIRY": {
        "presets": [
            "General information request",
            "Contact details request",
            "Office hours enquiry",
            "Service information request",
            "Application / process enquiry",
            "Status follow-up",
            "Referral to correct department",
            "Request for assistance",
        ],
        "other_label": "Other general enquiry",
    },

    "SERVICE ISSUE": {
        "presets": [
            "Service not provided",
            "Service delayed",
            "Poor service",
            "Repeated service failure",
            "Missed service request",
            "Incorrect service provided",
            "Service interruption",
            "Previous complaint unresolved",
        ],
        "other_label": "Other service issue",
    },

    "SECURITY": {
        "presets": [
            "Suspicious activity",
            "Vandalism",
            "Theft reported",
            "Break-in reported",
            "Trespassing",
            "Illegal occupation",
            "Public safety concern",
            "Damaged security infrastructure",
            "Streetlight safety concern",
        ],
        "other_label": "Other security issue",
    },

    "MAINTENANCE": {
        "presets": [
            "Pothole",
            "Road surface damaged",
            "Pavement damaged",
            "Kerb damaged",
            "Public building repair",
            "Fence / barrier damaged",
            "Signage damaged",
            "Stormwater infrastructure damaged",
            "Public facility maintenance",
            "Tree maintenance required",
        ],
        "other_label": "Other maintenance issue",
    },

    "MUNICIPAL SERVICE": {
        "presets": [
            "Water outage",
            "Water leak",
            "Low water pressure",
            "Electricity outage",
            "Streetlight not working",
            "Sewer blockage / overflow",
            "Stormwater drain blocked",
            "Refuse collection issue",
            "Municipal meter issue",
            "Road / traffic signal issue",
        ],
        "other_label": "Other municipal service issue",
    },

    "COMMUNITY MATTER": {
        "presets": [
            "Community complaint",
            "Neighbourhood concern",
            "Public meeting enquiry",
            "Community event enquiry",
            "Noise complaint",
            "Informal settlement concern",
            "Public space concern",
            "Community facility issue",
            "Request for community assistance",
            "Ward matter / referral",
        ],
        "other_label": "Other community matter",
    },

    "FINANCIAL": {
        "presets": [
            "Municipal account enquiry",
            "Incorrect charge",
            "Payment not reflected",
            "Outstanding balance enquiry",
            "Rates enquiry",
            "Utility billing enquiry",
            "Meter reading / billing query",
            "Refund enquiry",
            "Payment arrangement enquiry",
            "Statement request",
        ],
        "other_label": "Other financial issue",
    },
}

METHODS = {"TELEPHONE", "EMAIL", "WHATSAPP", "IN_PERSON", "INTERNAL", "OTHER"}
OUTCOMES = {"CONTACTED", "NO_ANSWER", "UNREACHABLE", "INFORMATION_RECEIVED", "ESCALATED", "NO_CONTACT_REQUIRED"}
NEXT_ACTIONS = {"NONE", "CONTACT", "REVIEW", "ASSIGN", "ESCALATE"}
REFERRAL_TRANSITIONS = {
    "CREATED": {"SUBMITTED", "CLOSED"},
    "SUBMITTED": {"ACKNOWLEDGED", "IN_PROGRESS", "RESPONSE_RECEIVED", "UNSUCCESSFUL", "ESCALATED"},
    "ACKNOWLEDGED": {"IN_PROGRESS", "RESPONSE_RECEIVED", "UNSUCCESSFUL", "ESCALATED"},
    "IN_PROGRESS": {"RESPONSE_RECEIVED", "RESOLVED", "UNSUCCESSFUL", "ESCALATED"},
    "RESPONSE_RECEIVED": {"IN_PROGRESS", "RESOLVED", "UNSUCCESSFUL", "ESCALATED"},
    "RESOLVED": {"CLOSED"}, "UNSUCCESSFUL": {"ESCALATED", "CLOSED"},
    "ESCALATED": {"SUBMITTED", "ACKNOWLEDGED", "IN_PROGRESS", "RESPONSE_RECEIVED", "UNSUCCESSFUL"},
    "RESOLVED_BY_CITY": {"CLOSED"}, "CLOSED": set(),
}


def timestamp(value=None, future=False):
    now = datetime.now(timezone.utc)
    if not value:
        return now
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            abort(400, description="Dates require a UTC offset, for example +00:00.")
        parsed = utc(parsed)
    except (ValueError, TypeError):
        abort(400, description="Invalid date/time.")
    if not future and parsed > now:
        abort(400, description="An event cannot have happened in the future.")
    return parsed


def text(value, maximum=4000, required=False):
    value = str(value or "").strip()
    if len(value) > maximum or (required and not value):
        abort(400, description="Missing or overlong text.")
    return value


def identifier(value):
    try:
        value = int(value)
    except (TypeError, ValueError):
        abort(400, description="Invalid record ID.")
    if value < 1:
        abort(400, description="Invalid record ID.")
    return value


def follow_up(org, actor, issue_id, values):
    audit.authorize(org, actor, providers.STAFF)
    issue = operations.issue(org, issue_id)
    operations.open_issue(issue)
    method, outcome, next_action = (values.get(k) for k in ("method", "outcome", "next_action"))
    if method not in METHODS or outcome not in OUTCOMES or next_action not in NEXT_ACTIONS:
        abort(400, description="Invalid follow-up state.")
    due = timestamp(values.get("next_action_at"), future=True) if values.get("next_action_at") else None
    if next_action != "NONE" and due is None:
        abort(400, description="A next action requires its due date.")
    row = UipFollowUp(organization_id=org, interaction_id=issue.id, actor_user_id=actor,
        occurred_at=timestamp(values.get("occurred_at")), method=method, outcome=outcome,
        next_action=next_action, next_action_at=due, note=text(values.get("note")))
    db.session.add(row)
    audit.record(org, actor, "follow_up.recorded", row)
    return row


def finish_follow_up(org, actor, row_id):
    audit.authorize(org, actor, providers.STAFF)
    row = UipFollowUp.query.filter_by(organization_id=org, id=row_id).populate_existing().with_for_update().first_or_404()
    if row.completed_at or row.next_action == "NONE":
        abort(409, description="There is no outstanding follow-up action.")
    row.completed_at, row.completed_by = datetime.now(timezone.utc), actor
    audit.record(org, actor, "follow_up.completed", row)
    return row


def referral(org, actor, issue_id, department, due=None, reference=None):
    audit.authorize(org, actor, ("manager", "receptionist", "committee_member"))
    issue = operations.issue(org, issue_id)
    operations.open_issue(issue)
    row = UipMunicipalReferral(organization_id=org, interaction_id=issue.id,
        department=text(department, 100, True), status="CREATED", version=1,
        municipality_reference=text(reference, 100),
        sla_expected_date=timestamp(due, future=True).replace(tzinfo=None) if due else None)
    db.session.add(row)
    db.session.flush()
    db.session.add(UipReferralEvent(organization_id=org, referral_id=row.id, actor_user_id=actor,
        occurred_at=datetime.now(timezone.utc), new_state="CREATED", resulting_version=1,
        municipality_reference=row.municipality_reference))
    audit.record(org, actor, "referral.created", row)
    return row


def transition_referral(org, actor, row_id, expected, destination, reference=None, note=None, occurred_at=None):
    audit.authorize(org, actor, ("manager", "receptionist", "committee_member"))
    row = UipMunicipalReferral.query.filter_by(organization_id=org, id=row_id).populate_existing().with_for_update().first_or_404()
    providers.version_matches(row, expected)
    reference_only = destination == row.status and reference and reference != row.municipality_reference and row.status != "CLOSED"
    if not reference_only and destination not in REFERRAL_TRANSITIONS.get(row.status, set()):
        abort(409, description="Invalid municipal referral transition.")
    at = timestamp(occurred_at)
    last = UipReferralEvent.query.filter_by(organization_id=org, referral_id=row.id).order_by(UipReferralEvent.resulting_version.desc()).first()
    if last and at < utc(last.occurred_at):
        abort(400, description="The event precedes the latest recorded transition.")
    reference = text(reference or row.municipality_reference, 100)
    if destination in {"ACKNOWLEDGED", "IN_PROGRESS", "RESPONSE_RECEIVED", "RESOLVED"} and not reference:
        abort(400, description="Capture the municipal reference before progressing.")
    note = text(note, required=destination in {"UNSUCCESSFUL", "ESCALATED", "CLOSED"})
    previous = row.status
    row.status, row.version, row.municipality_reference = destination, row.version + 1, reference
    if destination == "RESOLVED" and row.resolved_at is None:
        row.resolved_at = at.replace(tzinfo=None)
    db.session.add(UipReferralEvent(organization_id=org, referral_id=row.id, actor_user_id=actor,
        occurred_at=at, previous_state=previous, new_state=destination, resulting_version=row.version,
        municipality_reference=reference, note=note))
    audit.record(org, actor, "referral.transitioned", row)
    return row


def communication(org, actor, values):
    audit.authorize(org, actor, providers.STAFF)
    channel, direction, party, purpose = (values.get(k) for k in
        ("channel", "direction", "party_classification", "purpose"))
    if (channel not in METHODS or direction not in {"INBOUND", "OUTBOUND"}
        or party not in {"MEMBER", "PROVIDER", "MUNICIPALITY", "STAFF", "OTHER"}
        or purpose not in {"ACKNOWLEDGEMENT", "DISPATCH", "FOLLOW_UP", "INFORMATION", "GOVERNANCE", "OTHER"}):
        abort(400, description="Invalid communication classification.")
    links = {}
    for field, model in (("interaction_id", CoreInteraction), ("work_order_id", UipWorkOrder),
                         ("referral_id", UipMunicipalReferral)):
        if values.get(field):
            try:
                value = int(values[field])
            except (ValueError, TypeError):
                abort(400)
            row = model.query.filter_by(organization_id=org, id=value).first_or_404()
            links[field] = row.id
            if field != "interaction_id":
                if links.get("interaction_id", row.interaction_id) != row.interaction_id:
                    abort(400, description="Communication links must refer to the same issue.")
                links["interaction_id"] = row.interaction_id
    status = values.get("status", "RECORDED")
    if status not in {"RECORDED", "RECEIVED", "FAILED", "DELIVERY_UNAVAILABLE"}:
        abort(400)
    if status == "RECEIVED" and direction != "INBOUND":
        abort(400)
    if values.get("request_delivery"):
        status = "DELIVERY_UNAVAILABLE"
    row = UipCommunicationLog(organization_id=org, actor_user_id=actor,
        occurred_at=timestamp(values.get("occurred_at")), channel=channel, direction=direction,
        party_classification=party, purpose=purpose, status=status,
        summary=text(values.get("summary")), **links)
    db.session.add(row)
    audit.record(org, actor, "communication.recorded", row)
    return row


def relevant_issues(org, actor, issue_id):
    audit.authorize(org, actor, providers.STAFF)
    issue = operations.issue(org, issue_id)
    filters = []
    if issue.member_id:
        filters.append(CoreInteraction.member_id == issue.member_id)
    if issue.property_id:
        filters.append(CoreInteraction.property_id == issue.property_id)
    if not filters:
        return []
    return CoreInteraction.query.filter(CoreInteraction.organization_id == org,
        CoreInteraction.id != issue.id, db.or_(*filters)).order_by(CoreInteraction.id.desc()).limit(20).all()
