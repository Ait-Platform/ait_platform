"""UIP-only, tenant-scoped audit. The caller owns commit/rollback."""
from flask import abort
from app.extensions import db
from app.models.core import CoreOrganizationMember, CoreRoleAssignment, CoreRole

READ_ROLES = ("manager", "receptionist", "committee_member", "owner")
WRITE_ROLES = ("manager", "committee_member", "owner")
AUDIT_ROLES = ("manager", "committee_member", "owner")
SAFE_FIELDS = frozenset({
    "reference", "name", "member_type", "email", "phone", "is_active",
    "eligibility_status", "address", "rates_reference", "classification",
    "valid_from", "valid_to", "is_verified", "preference",
})
ACTIONS = {
    "member.created": ("UipMemberProfile", WRITE_ROLES),
    "member.updated": ("UipMemberProfile", WRITE_ROLES),
    "property.created": ("UipProperty", WRITE_ROLES),
    "property.updated": ("UipProperty", WRITE_ROLES),
    "ownership.created": ("UipPropertyMember", WRITE_ROLES),
    "ownership.updated": ("UipPropertyMember", WRITE_ROLES),
    "representation.created": ("UipMemberRepresentative", WRITE_ROLES),
    "representation.updated": ("UipMemberRepresentative", WRITE_ROLES),
    "preference.updated": ("UipCommunicationPreference", WRITE_ROLES),
    "interaction.created": ("CoreInteraction", ("manager", "receptionist", "committee_member")),
    "interaction.resolved": ("CoreInteraction", ("manager", "receptionist")),
    "task.created": ("CoreTask", ("manager", "receptionist")),
    "task.completed": ("CoreTask", ("manager", "receptionist")),
    "work_order.recorded": ("UipWorkOrder", ("manager", "committee_member")),
    "referral.recorded": ("UipMunicipalReferral", ("manager", "committee_member")),
    "organization.updated": ("CoreOrganization", ("manager", "committee_member", "owner")),
}

ACTIONS.update({
    "sla.configured": ("UipSlaPolicy", ("manager",)),
    "sla.deactivated": ("UipSlaPolicy", ("manager",)),
    "interaction.acknowledged": ("CoreInteraction", ("manager", "receptionist")),
    "task.cancelled": ("CoreTask", ("manager", "receptionist")),
    "provider.created": ("UipProvider", ("manager",)),
    "provider.updated": ("UipProvider", ("manager",)),
    "provider.deactivated": ("UipProvider", ("manager",)),
    "provider.user_linked": ("UipProviderUser", ("manager",)),
    "provider.user_revoked": ("UipProviderUser", ("manager",)),
    "work_order.created": ("UipWorkOrder", ("manager", "receptionist")),
    "work_order.dispatched": ("UipWorkOrder", ("manager", "receptionist")),
    **{f"work_order.{action}": ("UipWorkOrder", ("provider",)) for action in
       ("accepted", "started", "completed", "rejected", "failed")},
    **{f"work_order.{action}": ("UipWorkOrder", ("manager",)) for action in
       ("verified", "verification_rejected", "closed", "cancelled")},
})
STATE_VALUES = {"CREATED", "DISPATCHED", "ACCEPTED", "IN_PROGRESS", "COMPLETED", "VERIFIED",
                "CLOSED", "CANCELLED", "REJECTED", "FAILED", "pending", "completed", "cancelled"}
REASON_CODES = {"NOT_REQUIRED", "DUPLICATE", "UNABLE_TO_COMPLETE", "WORK_INCOMPLETE", "OTHER"}
DISPATCH_METHODS = {"TELEPHONE", "EMAIL", "IN_PERSON", "EXTERNAL_OTHER"}

OPERATIONAL_ACTIONS = {
    "follow_up.recorded": ("UipFollowUp", ("manager", "receptionist")),
    "follow_up.completed": ("UipFollowUp", ("manager", "receptionist")),
    "referral.created": ("UipMunicipalReferral", ("manager", "receptionist", "committee_member")),
    "referral.transitioned": ("UipMunicipalReferral", ("manager", "receptionist", "committee_member")),
    "communication.recorded": ("UipCommunicationLog", ("manager", "receptionist")),
    **{action: (model, ("manager", "committee_member")) for action, model in {
        "document.folder_created": "UipDocumentFolder", "document.created": "UipDocument",
        "document.replaced": "UipDocument", "document.metadata_updated": "UipDocument", "quorum.configured": "UipQuorumRule",
        "meeting.created": "UipCommitteeMeeting", "meeting.updated": "UipCommitteeMeeting",
        "meeting.cancelled": "UipCommitteeMeeting", "meeting.started": "UipCommitteeMeeting",
        "meeting.attendance": "UipMeetingParticipant", "meeting.concluded": "UipCommitteeMeeting",
        "survey.created": "UipSurvey", "survey.finalized": "UipSurvey",
        "decision.recorded": "UipResolution", "decision.status_recorded": "UipResolution",
    }.items()},
    "survey.responded": ("UipSurvey", ("manager", "committee_member", "owner", "resident")),
}
ACTIONS.update(OPERATIONAL_ACTIONS)


def authorize(organization_id, actor_user_id, roles):
    from app.models.auth import User
    account = db.session.get(User, actor_user_id)
    if not account or not account.is_active:
        abort(403)
    if not CoreOrganizationMember.query.filter_by(
        organization_id=organization_id, user_id=actor_user_id, is_active=True
    ).first():
        abort(403)
    assignment = CoreRoleAssignment.query.join(CoreRole, CoreRole.id == CoreRoleAssignment.role_id).filter(
        CoreRoleAssignment.organization_id == organization_id,
        CoreRoleAssignment.user_id == actor_user_id,
        CoreRole.slug.in_(roles),
        db.or_(CoreRole.organization_id.is_(None), CoreRole.organization_id == organization_id),
    ).first()
    if not assignment:
        abort(403)


def record(organization_id, actor_user_id, action, entity, metadata=None):
    from app.models.core import CoreInteraction, CoreOrganization, CoreTask
    from app.models.uip import (UipMemberProfile, UipProperty, UipPropertyMember,
        UipMemberRepresentative, UipCommunicationPreference, UipWorkOrder,
        UipMunicipalReferral, UipAuditEvent, UipProvider, UipProviderUser, UipSlaPolicy)
    models = {m.__name__: m for m in (CoreInteraction, CoreOrganization, CoreTask,
        UipMemberProfile, UipProperty, UipPropertyMember, UipMemberRepresentative,
        UipCommunicationPreference, UipWorkOrder, UipMunicipalReferral, UipProvider, UipProviderUser, UipSlaPolicy)}
    from app.models import uip as uip_models
    models.update({name: getattr(uip_models, name) for name, roles in OPERATIONAL_ACTIONS.values()})
    if action not in ACTIONS:
        raise ValueError("Unsupported UIP audit action")
    expected, roles = ACTIONS[action]
    authorize(organization_id, actor_user_id, roles)
    if type(entity) is not models[expected]:
        raise ValueError("Audit entity does not match action")
    entity_org = entity.id if expected == "CoreOrganization" else getattr(entity, "organization_id", None)
    if expected in ("CoreTask", "UipWorkOrder", "UipMunicipalReferral"):
        interaction = CoreInteraction.query.filter_by(
            id=entity.interaction_id, organization_id=organization_id).first()
        if not interaction:
            abort(404)
        entity_org = interaction.organization_id
    if entity_org != organization_id:
        abort(404)
    metadata = {} if metadata is None else metadata
    if not isinstance(metadata, dict):
        raise ValueError("Unsafe audit metadata")
    # Record field names only, never their values, contact data or message bodies.
    operational = action.startswith("work_order.") or action in {"task.completed", "task.cancelled"}
    allowed = {"changed_fields"} | ({"previous_state", "new_state", "version", "reason_code", "dispatch_method"} if operational else set())
    if set(metadata) - allowed:
        raise ValueError("Unsafe audit metadata")
    fields = metadata.get("changed_fields", [])
    if not isinstance(fields, list) or any(not isinstance(f, str) or f not in SAFE_FIELDS for f in fields):
        raise ValueError("Unsafe audit fields")
    for key, value in metadata.items():
        if key in {"previous_state", "new_state"} and value is not None and value not in STATE_VALUES:
            raise ValueError("Unsafe audit state")
        if key == "version" and (type(value) is not int or value < 1):
            raise ValueError("Unsafe audit version")
        if key == "reason_code" and value not in REASON_CODES:
            raise ValueError("Unsafe audit reason code")
        if key == "dispatch_method" and value not in DISPATCH_METHODS:
            raise ValueError("Unsafe audit dispatch method")
    db.session.flush()
    event = UipAuditEvent(organization_id=organization_id, actor_user_id=actor_user_id,
        action=action, entity_type=expected, entity_id=entity.id,
        metadata_json=dict(metadata))
    db.session.add(event)
    return event


def events(organization_id, actor_user_id, event_id=None):
    from app.models.uip import UipAuditEvent
    authorize(organization_id, actor_user_id, AUDIT_ROLES)
    query = UipAuditEvent.query.filter_by(organization_id=organization_id)
    if event_id is not None:
        return query.filter_by(id=event_id).first_or_404()
    return query.order_by(UipAuditEvent.created_at.desc(), UipAuditEvent.id.desc())
