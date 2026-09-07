"""Register operations. No commits: routes own the complete transaction."""
from datetime import date
from flask import abort
from app.extensions import db
from app.models.core import CoreOrganizationMember
from app.models.uip import (UipMemberProfile, UipProperty, UipPropertyMember,
    UipMemberRepresentative, UipCommunicationPreference)
from . import audit

CHANNELS = ("Telephone", "Email", "WhatsApp", "Post")


def identifier(value):
    try:
        value = int(value)
    except (TypeError, ValueError):
        abort(400, description="Invalid register record.")
    if value < 1:
        abort(400, description="Invalid register record.")
    return value


def get(model, organization_id, actor_user_id, record_id):
    audit.authorize(organization_id, actor_user_id, audit.READ_ROLES)
    return model.query.filter_by(id=identifier(record_id), organization_id=organization_id).first_or_404()


def members(organization_id, actor_user_id, active_only=False):
    audit.authorize(organization_id, actor_user_id, audit.READ_ROLES)
    query = UipMemberProfile.query.filter_by(organization_id=organization_id)
    if active_only:
        query = query.filter_by(is_active=True)
    return query.order_by(UipMemberProfile.name, UipMemberProfile.id)


def properties(organization_id, actor_user_id, active_only=False):
    audit.authorize(organization_id, actor_user_id, audit.READ_ROLES)
    query = UipProperty.query.filter_by(organization_id=organization_id)
    if active_only:
        query = query.filter_by(is_active=True)
    return query.order_by(UipProperty.reference, UipProperty.id)


def available_memberships(organization_id, actor_user_id):
    audit.authorize(organization_id, actor_user_id, audit.WRITE_ROLES)
    return CoreOrganizationMember.query.filter(
        CoreOrganizationMember.organization_id == organization_id,
        CoreOrganizationMember.user_id.isnot(None),
        CoreOrganizationMember.is_active.is_(True),
        ~db.exists().where(UipMemberProfile.membership_id == CoreOrganizationMember.id,
                           UipMemberProfile.organization_id == organization_id),
    ).order_by(CoreOrganizationMember.id).all()


def text(data, key, limit, required=False):
    value = (data.get(key) or "").strip()
    if len(value) > limit or (required and not value):
        abort(400, description="Please complete the register fields within their displayed limits.")
    return value or None


def choice(data, key, values):
    value = data.get(key)
    if value not in values:
        abort(400, description="Invalid register option.")
    return value


def boolean(data, key):
    return choice(data, key, ("true", "false")) == "true"


def save_member(organization_id, actor_user_id, data, member_id=None):
    audit.authorize(organization_id, actor_user_id, audit.WRITE_ROLES)
    values = {
        "reference": text(data, "reference", 50, True), "name": text(data, "name", 255, True),
        "member_type": choice(data, "member_type", ("person", "business")),
        "email": text(data, "email", 255), "phone": text(data, "phone", 50),
        "is_active": boolean(data, "is_active"),
        "eligibility_status": choice(data, "eligibility_status", ("unverified", "eligible", "ineligible")),
    }
    if values["email"] and ("@" not in values["email"] or any(c.isspace() for c in values["email"])):
        abort(400, description="Enter a valid contact email.")
    if member_id is None:
        link = data.get("membership_id")
        if link:
            membership = CoreOrganizationMember.query.filter_by(
                id=identifier(link), organization_id=organization_id, is_active=True
            ).with_for_update().first_or_404()
            if membership.user_id is None or UipMemberProfile.query.filter_by(organization_id=organization_id, membership_id=membership.id).first():
                abort(400, description="Choose an unlinked existing account membership.")
        else:
            # A non-login member receives neither a User account nor any role.
            membership = CoreOrganizationMember(organization_id=organization_id, user_id=None, is_active=True)
            db.session.add(membership)
            db.session.flush()
        member = UipMemberProfile(organization_id=organization_id, membership_id=membership.id)
        db.session.add(member)
        action = "member.created"
    else:
        member = get(UipMemberProfile, organization_id, actor_user_id, member_id)
        # Account association is immutable here: never repurpose access membership.
        if data.get("membership_id") and identifier(data["membership_id"]) != member.membership_id:
            abort(400, description="Account association cannot be changed here.")
        action = "member.updated"
    changed = [key for key, value in values.items() if getattr(member, key) != value]
    for key, value in values.items():
        setattr(member, key, value)
    audit.record(organization_id, actor_user_id, action, member, {"changed_fields": changed})
    return member


def save_property(organization_id, actor_user_id, data, property_id=None):
    audit.authorize(organization_id, actor_user_id, audit.WRITE_ROLES)
    values = {
        "reference": text(data, "reference", 50, True), "address": text(data, "address", 500, True),
        "rates_reference": text(data, "rates_reference", 100),
        "classification": choice(data, "classification", ("residential", "business", "mixed", "other")),
        "is_active": boolean(data, "is_active"),
    }
    item = get(UipProperty, organization_id, actor_user_id, property_id) if property_id else UipProperty(organization_id=organization_id)
    changed = [key for key, value in values.items() if getattr(item, key) != value]
    for key, value in values.items():
        setattr(item, key, value)
    db.session.add(item)
    audit.record(organization_id, actor_user_id, "property.updated" if property_id else "property.created",
                 item, {"changed_fields": changed})
    return item


def dates(data):
    try:
        start = date.fromisoformat(data.get("valid_from", ""))
        end = date.fromisoformat(data["valid_to"]) if data.get("valid_to") else None
    except (TypeError, ValueError):
        abort(400, description="Enter valid effective dates.")
    if end and end < start:
        abort(400, description="The end date cannot precede the start date.")
    return start, end


def save_relationship(organization_id, actor_user_id, data, property_id=None, member_id=None, link_id=None):
    audit.authorize(organization_id, actor_user_id, audit.WRITE_ROLES)
    start, end = dates(data)
    verified = boolean(data, "is_verified")
    if property_id is not None:
        parent = get(UipProperty, organization_id, actor_user_id, property_id)
        model, action = UipPropertyMember, "ownership"
        member = get(UipMemberProfile, organization_id, actor_user_id, data.get("member_id"))
        relationship = choice(data, "relationship", ("owner", "occupier", "representative"))
        values = dict(property_id=parent.id, member_id=member.id, relationship=relationship)
    else:
        parent = get(UipMemberProfile, organization_id, actor_user_id, member_id)
        model, action = UipMemberRepresentative, "representation"
        representative = get(UipMemberProfile, organization_id, actor_user_id, data.get("representative_id"))
        if parent.id == representative.id:
            abort(400, description="A member cannot represent itself.")
        values = dict(member_id=parent.id, representative_id=representative.id)
    if link_id:
        item = get(model, organization_id, actor_user_id, link_id)
        if any(getattr(item, key) != value for key, value in values.items()):
            abort(400, description="Existing relationship parties cannot be changed.")
    else:
        item = model(organization_id=organization_id, **values)
        db.session.add(item)
    item.valid_from, item.valid_to, item.is_verified = start, end, verified
    audit.record(organization_id, actor_user_id, action + (".updated" if link_id else ".created"), item,
                 {"changed_fields": ["valid_from", "valid_to", "is_verified"]})
    return item


def set_preference(organization_id, actor_user_id, member_id, data):
    audit.authorize(organization_id, actor_user_id, audit.WRITE_ROLES)
    member = get(UipMemberProfile, organization_id, actor_user_id, member_id)
    channel = choice(data, "channel", CHANNELS)
    value = choice(data, "preference", ("unspecified", "allowed", "declined"))
    item = UipCommunicationPreference.query.filter_by(
        organization_id=organization_id, member_id=member.id, channel=channel).first()
    if not item:
        item = UipCommunicationPreference(organization_id=organization_id, member_id=member.id, channel=channel)
        db.session.add(item)
    item.preference = value
    audit.record(organization_id, actor_user_id, "preference.updated", item, {"changed_fields": ["preference"]})
    return item


def intake_links(organization_id, actor_user_id, member_id=None, property_id=None):
    audit.authorize(organization_id, actor_user_id, ("manager", "receptionist", "committee_member"))
    member = get(UipMemberProfile, organization_id, actor_user_id, member_id) if member_id else None
    item = get(UipProperty, organization_id, actor_user_id, property_id) if property_id else None
    if (member and not member.is_active) or (item and not item.is_active):
        abort(400, description="Choose active register records.")
    # Reporting an issue at a property does not assert ownership or voting rights.
    return member, item
