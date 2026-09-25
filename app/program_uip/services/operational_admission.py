"""Secretary verification of existing Staff/Provider claims; no provisioning system."""
from datetime import datetime, timezone
from flask import abort
from sqlalchemy import func
from app.extensions import db
from app.models.auth import User
from app.models.core import CoreInteraction, CoreOrganization, CoreOrganizationMember, CoreRole, CoreRoleAssignment
from app.models.uip import UipAuditEvent
from app.models.uip_governance import UipCommitteeMember


def require_secretary(org, actor):
    user = db.session.get(User, actor)
    if not user or not user.is_active or not (user.email or "").strip():
        abort(403)
    member = UipCommitteeMember.query.filter(
        UipCommitteeMember.organization_id == org, UipCommitteeMember.status == "CURRENT",
        func.lower(func.trim(UipCommitteeMember.position)) == "secretary",
        func.lower(func.trim(UipCommitteeMember.email)) == user.email.strip().lower()).first()
    if not member:
        abort(403, description="Staff and Provider admission requires the current Secretary.")


def permitted_roles(claim):
    if claim.interaction_type != "staff_claim":
        return ()
    if claim.category == "UIP_STAFF_ACCESS" or claim.description == "Requested operational journey: staff":
        return ("receptionist",)
    # Older combined requests require an explicit Secretary choice; never default to owner.
    return ("receptionist",)


def admit(org, actor, claim_ids, values):
    require_secretary(org, actor)
    try:
        ids = {int(value) for value in claim_ids}
    except (TypeError, ValueError):
        abort(400)
    if not ids:
        abort(400)
    # Serialize repeated admissions and membership/role reuse in this organisation.
    CoreOrganization.query.filter_by(id=org).with_for_update().one()
    claims = CoreInteraction.query.filter(CoreInteraction.organization_id == org,
        CoreInteraction.id.in_(ids)).with_for_update().all()
    if len(claims) != len(ids):
        abort(404)
    selected = []
    for claim in claims:
        role_slug = values.get(f"operational_role_{claim.id}")
        if role_slug not in permitted_roles(claim) or claim.creator_id == actor:
            abort(403)
        if claim.status not in ("OPEN", "VERIFIED"):
            abort(409)
        user = db.session.get(User, claim.creator_id)
        if not user or not user.is_active:
            abort(403)
        selected.append((claim, user, role_slug))
    for claim, user, role_slug in selected:
        if claim.status == "VERIFIED":
            event = UipAuditEvent.query.filter_by(organization_id=org, action="OPERATIONAL_ACCESS_VERIFIED",
                entity_type="CoreInteraction", entity_id=claim.id).first()
            if not event or event.metadata_json.get("role") != role_slug:
                abort(409)
            continue
        member = CoreOrganizationMember.query.filter_by(organization_id=org, user_id=user.id).first()
        if member is None:
            member = CoreOrganizationMember(organization_id=org, user_id=user.id, is_active=True)
            db.session.add(member)
        else:
            member.is_active = True
        role = CoreRole.query.filter_by(organization_id=org, slug=role_slug).first()
        if role is None:
            role = CoreRole.query.filter_by(organization_id=None, slug=role_slug).first()
        if role is None:
            role = CoreRole(organization_id=org, slug=role_slug, name=role_slug.title())
            db.session.add(role); db.session.flush()
        if not CoreRoleAssignment.query.filter_by(organization_id=org, user_id=user.id, role_id=role.id).first():
            db.session.add(CoreRoleAssignment(organization_id=org, user_id=user.id, role_id=role.id))
        claim.status = "VERIFIED"
        claim.closed_by = actor
        claim.closed_at = datetime.now(timezone.utc)
        db.session.add(UipAuditEvent(organization_id=org, actor_user_id=actor,
            action="OPERATIONAL_ACCESS_VERIFIED", entity_type="CoreInteraction", entity_id=claim.id,
            metadata_json={"role": role_slug, "admitted_user_id": user.id}))
    return claims
