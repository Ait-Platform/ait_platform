"""Recorded external work: issue lock -> provider lock -> order lock; no commits."""
import hashlib
import json
import uuid
from datetime import datetime, timezone
from flask import abort
from werkzeug.exceptions import Forbidden
from app.extensions import db
from app.models.uip import UipWorkOrder, UipWorkOrderAction
from . import audit, providers, operations

# action: (legal sources, destination, actor roles, reason required)
TRANSITIONS = {
    "dispatched": ({"CREATED"}, "DISPATCHED", providers.STAFF, False),
    "accepted": ({"DISPATCHED"}, "ACCEPTED", ("provider",), False),
    "started": ({"ACCEPTED"}, "IN_PROGRESS", ("provider",), False),
    "completed": ({"IN_PROGRESS"}, "COMPLETED", ("provider",), False),
    "verified": ({"COMPLETED"}, "VERIFIED", ("manager",), False),
    "verification_rejected": ({"COMPLETED"}, "IN_PROGRESS", ("manager",), True),
    "closed": ({"VERIFIED"}, "CLOSED", ("manager",), False),
    "rejected": ({"DISPATCHED"}, "REJECTED", ("provider",), True),
    "failed": ({"ACCEPTED", "IN_PROGRESS"}, "FAILED", ("provider",), True),
    "cancelled": ({"CREATED", "DISPATCHED", "ACCEPTED", "IN_PROGRESS"}, "CANCELLED", ("manager",), True),
}


def is_staff(org, actor):
    try:
        audit.authorize(org, actor, providers.STAFF)
        return True
    except Forbidden:
        return False


def get(org, actor, order_id):
    order = UipWorkOrder.query.filter_by(organization_id=org, id=order_id).first_or_404()
    if not is_staff(org, actor):
        audit.authorize(org, actor, ("provider",))
        if not providers.linked(org, order.provider_id, actor) or not UipWorkOrderAction.query.filter_by(
            organization_id=org, work_order_id=order.id, action="dispatched").first():
            abort(404)
    return order


def orders(org, actor):
    query = UipWorkOrder.query.filter_by(organization_id=org)
    if not is_staff(org, actor):
        from app.models.core import CoreOrganizationMember
        from app.models.uip import UipProviderUser
        audit.authorize(org, actor, ("provider",))
        linked_ids = db.session.query(UipProviderUser.provider_id).join(CoreOrganizationMember,
            CoreOrganizationMember.id == UipProviderUser.membership_id).filter(
            UipProviderUser.organization_id == org, UipProviderUser.is_active.is_(True),
            CoreOrganizationMember.organization_id == org, CoreOrganizationMember.user_id == actor,
            CoreOrganizationMember.is_active.is_(True))
        dispatched = db.session.query(UipWorkOrderAction.work_order_id).filter_by(organization_id=org, action="dispatched")
        query = query.filter(UipWorkOrder.provider_id.in_(linked_ids), UipWorkOrder.id.in_(dispatched))
    return query.order_by(UipWorkOrder.id.desc())


def projection(org, actor, order_id):
    order = get(org, actor, order_id)
    # Explicit projection: never pass the ORM issue/member/property/task graph to a provider template.
    return dict(id=order.id, reference=order.reference, description=order.description,
                service_location=order.service_location, priority=order.interaction.priority,
                status=order.status, version=order.version)


def _request(org, actor, key, payload):
    try:
        key = str(uuid.UUID(str(key)))
    except (ValueError, TypeError, AttributeError):
        abort(400, description="A UUID request key is required.")
    fingerprint = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    # Serialise the same actor/request across issues too. Hash collisions only cause waiting.
    lock = int.from_bytes(hashlib.sha256(f"{org}:{actor}:{key}".encode()).digest()[:8], "big", signed=True)
    db.session.execute(db.text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock})
    old = UipWorkOrderAction.query.filter_by(organization_id=org, actor_user_id=actor, request_key=key).first()
    if old and old.fingerprint != fingerprint:
        abort(409, description="Request key was already used with a different payload.")
    return key, fingerprint, old


def _journal(org, actor, order, action, previous, key, fingerprint, note=None, reason_code=None, dispatch_method=None):
    metadata = {"previous_state": previous, "new_state": order.status, "version": order.version}
    if reason_code:
        metadata["reason_code"] = reason_code
    if dispatch_method:
        metadata["dispatch_method"] = dispatch_method
    event = audit.record(org, actor, "work_order." + action, order, metadata)
    db.session.flush()
    row = UipWorkOrderAction(organization_id=org, work_order_id=order.id, actor_user_id=actor,
        action=action, previous_state=previous, new_state=order.status, resulting_version=order.version,
        request_key=key, fingerprint=fingerprint, note=note, reason_code=reason_code,
        dispatch_method=dispatch_method, audit_event_id=event.id)
    db.session.add(row)
    db.session.flush()
    return row


def create(org, actor, issue_id, provider_id, description, service_location, request_key):
    audit.authorize(org, actor, providers.STAFF)
    description, service_location = (description or "").strip(), (service_location or "").strip()
    if not description or len(description) > 4000 or not service_location or len(service_location) > 500:
        abort(400, description="Approved work scope and service location are required.")
    key, fingerprint, old = _request(org, actor, request_key,
        dict(action="created", issue_id=issue_id, provider_id=provider_id, description=description, service_location=service_location))
    if old:
        return get(org, actor, old.work_order_id)
    ix = operations.issue(org, issue_id)
    operations.open_issue(ix)
    provider = providers.get(org, provider_id, True)
    if not providers.eligible(org, provider, ix.category):
        abort(409, description="Provider requires availability, matching capability and an authorised user association.")
    if UipWorkOrder.query.filter(UipWorkOrder.organization_id == org, UipWorkOrder.interaction_id == ix.id,
                                UipWorkOrder.status.notin_(providers.TERMINAL)).first():
        abort(409, description="This issue already has a nonterminal provider order.")
    order = UipWorkOrder(organization_id=org, interaction_id=ix.id, provider_id=provider.id,
        created_by=actor, reference="WO-" + uuid.uuid4().hex.upper(), description=description,
        service_location=service_location, status="CREATED", version=1, state_changed_at=datetime.now(timezone.utc))
    db.session.add(order)
    if ix.status == "NEW":
        ix.status = "IN_PROGRESS"
    _journal(org, actor, order, "created", None, key, fingerprint)
    return order


def transition(org, actor, order_id, action, expected_version, request_key, note=None, reason_code=None, dispatch_method=None):
    if action not in TRANSITIONS:
        abort(409, description="Invalid work-order action.")
    sources, destination, roles, reason_required = TRANSITIONS[action]
    audit.authorize(org, actor, roles)
    candidate = get(org, actor, order_id)
    if roles == ("provider",) and not providers.linked(org, candidate.provider_id, actor):
        abort(403)
    if action in {"verified", "verification_rejected", "closed"} and providers.associated(org, candidate.provider_id, actor):
        abort(403, description="A manager associated with this provider cannot verify or close its work.")
    note, reason_code, dispatch_method = (note or "").strip(), reason_code or None, dispatch_method or None
    if len(note) > 4000 or (reason_required and (not note or reason_code not in audit.REASON_CODES)):
        abort(400, description="A valid reason code and operational reason are required.")
    if reason_code and reason_code not in audit.REASON_CODES:
        abort(400)
    if action == "dispatched" and dispatch_method not in audit.DISPATCH_METHODS:
        abort(400, description="Record the actual external dispatch method.")
    if action != "dispatched" and dispatch_method:
        abort(400)
    try:
        expected_version = int(expected_version)
    except (ValueError, TypeError):
        abort(400)
    key, fingerprint, old = _request(org, actor, request_key, dict(order_id=order_id, action=action,
        expected_version=expected_version, note=note, reason_code=reason_code, dispatch_method=dispatch_method))
    if old:
        return get(org, actor, old.work_order_id)
    ix = operations.issue(org, candidate.interaction_id)
    provider = providers.get(org, candidate.provider_id, True)
    order = UipWorkOrder.query.filter_by(organization_id=org, id=order_id).populate_existing().with_for_update().one()
    # Recheck link under the provider lock: revocation must take effect immediately.
    audit.authorize(org, actor, roles)
    if roles == ("provider",) and not providers.linked(org, provider.id, actor):
        abort(403)
    if action in {"verified", "verification_rejected", "closed"} and providers.associated(org, provider.id, actor):
        abort(403)
    providers.version_matches(order, expected_version)
    if order.status not in sources:
        abort(409, description="Invalid transition from the current work-order state.")
    operations.open_issue(ix)
    if action == "dispatched" and not providers.eligible(org, provider, ix.category):
        abort(409, description="Provider is no longer eligible for dispatch.")
    if action == "closed":
        operations.resolve(org, actor, ix.id, verified_order=order)
    previous = order.status
    order.status, order.version = destination, order.version + 1
    order.state_changed_at = datetime.now(timezone.utc)
    if action == "completed":
        order.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
    if action == "verified":
        order.verified_at = datetime.now(timezone.utc).replace(tzinfo=None)
    _journal(org, actor, order, action, previous, key, fingerprint, note, reason_code, dispatch_method)
    return order
