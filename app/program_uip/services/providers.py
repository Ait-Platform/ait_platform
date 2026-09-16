"""Organisation-scoped manual provider register. Caller owns the transaction."""
from datetime import datetime, timezone
from flask import abort
from werkzeug.exceptions import Forbidden
from app.extensions import db
from app.models.auth import User
from app.models.core import CoreOrganizationMember
from app.models.uip import UipProvider, UipProviderCapability, UipProviderUser, UipWorkOrder
from . import audit

CATEGORIES = {"GENERAL ENQUIRY", "SERVICE ISSUE", "SECURITY", "CLEANING", "MAINTENANCE",
              "MUNICIPAL SERVICE", "COMMUNITY MATTER", "FINANCIAL"}
TERMINAL = {"CLOSED", "CANCELLED", "REJECTED", "FAILED"}
STAFF = ("manager", "receptionist")


def version_matches(entity, expected):
    try:
        expected = int(expected)
    except (ValueError, TypeError):
        abort(400, description="An expected version is required.")
    if entity.version != expected:
        abort(409, description="This record changed. Reload before trying again.")


def get(org, provider_id, lock=False):
    query = UipProvider.query.filter_by(organization_id=org, id=provider_id)
    return (query.populate_existing().with_for_update() if lock else query).first_or_404()


def linked(org, provider_id, user_id):
    # A role is never a substitute for an explicit, currently authorised association.
    try:
        audit.authorize(org, user_id, ("provider",))
    except Forbidden:
        return False
    return UipProviderUser.query.join(CoreOrganizationMember,
        CoreOrganizationMember.id == UipProviderUser.membership_id).filter(
        UipProviderUser.organization_id == org, UipProviderUser.provider_id == provider_id,
        UipProviderUser.is_active.is_(True), CoreOrganizationMember.organization_id == org,
        CoreOrganizationMember.user_id == user_id, CoreOrganizationMember.is_active.is_(True)).first() is not None


def associated(org, provider_id, user_id):
    # Conflict-of-interest guard does not depend on holding a provider role.
    return UipProviderUser.query.join(CoreOrganizationMember,
        CoreOrganizationMember.id == UipProviderUser.membership_id).filter(
        UipProviderUser.organization_id == org, UipProviderUser.provider_id == provider_id,
        UipProviderUser.is_active.is_(True), CoreOrganizationMember.organization_id == org,
        CoreOrganizationMember.user_id == user_id).first() is not None


def eligible(org, provider, category):
    if not provider.is_active or provider.availability != "AVAILABLE":
        return False
    if not UipProviderCapability.query.filter_by(organization_id=org, provider_id=provider.id, category=category).first():
        return False
    members = CoreOrganizationMember.query.join(UipProviderUser,
        UipProviderUser.membership_id == CoreOrganizationMember.id).filter(
        CoreOrganizationMember.organization_id == org, UipProviderUser.organization_id == org,
        UipProviderUser.provider_id == provider.id, UipProviderUser.is_active.is_(True)).all()
    return any(m.user_id and linked(org, provider.id, m.user_id) for m in members)


def save(org, actor, values, categories, provider_id=None, expected=None):
    audit.authorize(org, actor, ("manager",))
    provider = get(org, provider_id, True) if provider_id else UipProvider(organization_id=org, version=1)
    if provider_id:
        version_matches(provider, expected)
    cleaned = {}
    for field, limit in (("name",255),("contact_email",255),("contact_phone",50)):
        cleaned[field] = str(values.get(field) or "").strip()
        if len(cleaned[field]) > limit or (field == "name" and not cleaned[field]):
            abort(400, description="Invalid provider details.")
    availability = values.get("availability", "UNKNOWN")
    if availability not in {"UNKNOWN", "AVAILABLE", "UNAVAILABLE"} or not set(categories) <= CATEGORIES:
        abort(400, description="Invalid availability or capability.")
    for field,value in cleaned.items():
        setattr(provider,field,value)
    provider.availability = availability
    provider.updated_at = datetime.now(timezone.utc)
    if provider_id:
        provider.version += 1
        UipProviderCapability.query.filter_by(organization_id=org, provider_id=provider.id).delete()
    else:
        provider.is_active = True
        db.session.add(provider)
    db.session.flush()
    for category in sorted(set(categories)):
        db.session.add(UipProviderCapability(organization_id=org, provider_id=provider.id, category=category))
    audit.record(org, actor, "provider.updated" if provider_id else "provider.created", provider)
    return provider


def deactivate(org, actor, provider_id, expected):
    audit.authorize(org, actor, ("manager",))
    provider = get(org, provider_id, True)
    version_matches(provider, expected)
    if UipWorkOrder.query.filter(UipWorkOrder.organization_id == org,
        UipWorkOrder.provider_id == provider.id, UipWorkOrder.status.notin_(TERMINAL)).first():
        abort(409, description="Provider has outstanding work orders.")
    provider.is_active = False
    provider.version += 1
    provider.updated_at = datetime.now(timezone.utc)
    audit.record(org, actor, "provider.deactivated", provider)
    return provider


def associate(org, actor, provider_id, membership_id, expected):
    audit.authorize(org, actor, ("manager",))
    provider = get(org, provider_id, True)
    version_matches(provider, expected)
    member = CoreOrganizationMember.query.filter_by(id=membership_id, organization_id=org, is_active=True).first_or_404()
    if not provider.is_active or not member.user_id:
        abort(400, description="An active provider and user membership are required.")
    audit.authorize(org, member.user_id, ("provider",))
    if UipProviderUser.query.filter_by(organization_id=org, provider_id=provider.id,
                                      membership_id=member.id, is_active=True).first():
        abort(409, description="Association already active.")
    link = UipProviderUser(organization_id=org, provider_id=provider.id, membership_id=member.id, linked_by=actor)
    db.session.add(link)
    provider.version += 1
    provider.updated_at = datetime.now(timezone.utc)
    audit.record(org, actor, "provider.user_linked", link)
    return link


def revoke(org, actor, provider_id, link_id, expected):
    audit.authorize(org, actor, ("manager",))
    provider = get(org, provider_id, True)
    version_matches(provider, expected)
    link = UipProviderUser.query.filter_by(id=link_id, organization_id=org, provider_id=provider.id, is_active=True).first_or_404()
    link.is_active = False
    link.revoked_by, link.revoked_at = actor, datetime.now(timezone.utc)
    provider.version += 1
    provider.updated_at = datetime.now(timezone.utc)
    audit.record(org, actor, "provider.user_revoked", link)
    return link
