"""Register operations. No commits: routes own the complete transaction."""
from datetime import date
from flask import abort
from werkzeug.exceptions import BadRequest
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


def require_register_admin(organization_id, actor_user_id):
    from . import audit
    audit.authorize(organization_id, actor_user_id, ("manager", "RATEPAYER_ADMIN"))
    return True

def require_mo_vault_import(organization_id, actor_user_id):
    from . import audit
    audit.authorize(organization_id, actor_user_id, ("municipal_officer",))
    return True

def require_register_write(organization_id, actor_user_id):
    from . import audit
    audit.authorize(organization_id, actor_user_id, ("manager", "RATEPAYER_ADMIN", "municipal_officer"))
    return True

def available_memberships(organization_id, actor_user_id):
    require_register_write(organization_id, actor_user_id)
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


class InvalidRegisterOption(BadRequest):
    """Carry the exact failed choice for CSV diagnostics; manual UI stays unchanged."""
    def __init__(self, column, value, allowed):
        super().__init__(description="Invalid register option.")
        self.column, self.value, self.allowed = column, value, tuple(allowed)


def choice(data, key, values, default=None):
    value = data.get(key)
    if not value and default is not None:
        return default
    if value and isinstance(value, str):
        value = value.strip().lower()
    if value not in values:
        if default is not None:
            return default
        raise InvalidRegisterOption(key, value, values)
    return value


def boolean(data, key, default=True):
    val = data.get(key)
    if not val:
        return default
    val = str(val).strip().lower()
    if val in ('true', 't', 'yes', 'y', '1'):
        return True
    if val in ('false', 'f', 'no', 'n', '0'):
        return False
    return default


def save_member(organization_id, actor_user_id, data, member_id=None, is_import=False, import_id=None, is_authoritative=False):
    require_register_write(organization_id, actor_user_id)
    if not is_import and member_id is None:
        abort(403, description="Manual creation of authoritative ratepayers is prohibited.")
        
    values = {
        "reference": text(data, "reference", 50, True), "name": text(data, "name", 255, True),
        "member_type": choice(data, "member_type", ("person", "business"), default="person"),
        "email": text(data, "email", 255), "phone": text(data, "phone", 50),
        "is_active": boolean(data, "is_active", default=True),
        "eligibility_status": choice(data, "eligibility_status", ("unverified", "eligible", "ineligible"), default="eligible"),
    }
    
    if is_import and member_id is not None and not is_authoritative:
        # Protect operational fields from being overwritten by absent municipal fields
        values.pop("email", None)
        values.pop("phone", None)
        values.pop("eligibility_status", None)
        
    if values.get("email") and ("@" not in values["email"] or any(c.isspace() for c in values["email"])):
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
        member = UipMemberProfile(organization_id=organization_id, membership_id=membership.id, record_source="MUNICIPAL" if is_authoritative else "MANUAL")
        if import_id:
            member.last_import_id = import_id
        db.session.add(member)
        action = "member.created"
    else:
        member = get(UipMemberProfile, organization_id, actor_user_id, member_id)
        if data.get("membership_id") and identifier(data["membership_id"]) != member.membership_id:
            abort(400, description="Account association cannot be changed here.")
            
        if not is_import and getattr(member, 'record_source', 'MANUAL') == 'MUNICIPAL':
            for sealed in ("reference", "name", "member_type"):
                if sealed in values and getattr(member, sealed) != values[sealed]:
                    abort(403, description=f"Cannot manually modify sealed municipal field '{sealed}'.")
                    
        if is_authoritative:
            member.record_source = "MUNICIPAL"
            if import_id:
                member.last_import_id = import_id
                
        action = "member.updated"
    changed = [key for key, value in values.items() if getattr(member, key) != value]
    for key, value in values.items():
        setattr(member, key, value)
    metadata = {"changed_fields": changed}
    if import_id:
        metadata["import_id"] = import_id
    audit.record(organization_id, actor_user_id, action, member, metadata)
    return member


def save_property(organization_id, actor_user_id, data, property_id=None, is_import=False, import_id=None, is_authoritative=False):
    require_register_write(organization_id, actor_user_id)
    if not is_import and property_id is None:
        abort(403, description="Manual creation of authoritative properties is prohibited.")
        
    values = {
        "reference": text(data, "reference", 50, True), "address": text(data, "address", 500, True),
        "rates_reference": text(data, "rates_reference", 100),
        "classification": choice(data, "classification", ("residential", "business", "mixed", "other"), default="residential"),
        "is_active": boolean(data, "is_active", default=True),
    }
    
    if property_id:
        item = get(UipProperty, organization_id, actor_user_id, property_id)
        if not is_import and getattr(item, 'record_source', 'MANUAL') == 'MUNICIPAL':
            for sealed in ("reference", "address", "rates_reference", "classification"):
                if sealed in values and getattr(item, sealed) != values[sealed]:
                    abort(403, description=f"Cannot manually modify sealed municipal field '{sealed}'.")
    else:
        item = UipProperty(organization_id=organization_id, record_source="MUNICIPAL" if is_authoritative else "MANUAL")
        db.session.add(item)
        
    if is_authoritative:
        if hasattr(item, "record_source"):
            item.record_source = "MUNICIPAL"
        if import_id:
            item.last_import_id = import_id
            
    changed = [key for key, value in values.items() if getattr(item, key) != value]
    for key, value in values.items():
        setattr(item, key, value)
    metadata = {"changed_fields": changed}
    if import_id:
        metadata["import_id"] = import_id
    audit.record(organization_id, actor_user_id, "property.updated" if property_id else "property.created",
                 item, metadata)
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


def save_relationship(organization_id, actor_user_id, data, property_id=None, member_id=None, link_id=None, is_import=False, import_id=None, is_authoritative=False):
    require_register_write(organization_id, actor_user_id)
    start, end = dates(data)
    verified = boolean(data, "is_verified")
    
    if property_id is not None:
        parent = get(UipProperty, organization_id, actor_user_id, property_id)
        model, action = UipPropertyMember, "ownership"
        member = get(UipMemberProfile, organization_id, actor_user_id, data.get("member_id"))
        relationship = choice(data, "relationship", ("owner", "occupier", "representative"))
        
        if not is_import and relationship != "representative":
            abort(403, description="Manual creation or alteration of authoritative municipal ratepayer/property relationships is prohibited.")
            
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
        if not is_import and getattr(item, 'record_source', 'MANUAL') == 'MUNICIPAL':
            abort(403, description="Cannot manually alter a sealed municipal relationship.")
            
        if any(getattr(item, key) != value for key, value in values.items()):
            abort(400, description="Existing relationship parties cannot be changed.")
        if start != item.valid_from or (item.valid_to is not None and end != item.valid_to):
            abort(400, description="Historical relationship dates cannot be rewritten. Keep the original start/end and add a new dated relationship.")
    else:
        existing = model.query.filter_by(organization_id=organization_id, **values).filter(
            db.or_(model.valid_to.is_(None), model.valid_to >= start))
        for existing_item in existing:
            if existing_item.is_verified == verified:
                abort(400, description="This relationship is already recorded for that time period.")
        item_kwargs = dict(organization_id=organization_id, **values)
        if model is UipPropertyMember:
            item_kwargs["record_source"] = "MUNICIPAL" if is_authoritative else "MANUAL"
        item = model(**item_kwargs)
        db.session.add(item)
        
    if is_authoritative:
        if hasattr(item, "record_source"):
            item.record_source = "MUNICIPAL"
        if import_id:
            item.last_import_id = import_id
            
    item.valid_from, item.valid_to = start, end
    changed = ["is_verified", "valid_from", "valid_to"]
    if item.is_verified != verified:
        item.is_verified = verified
    else:
        changed.remove("is_verified")
        
    metadata = {"changed_fields": changed}
    if import_id:
        metadata["import_id"] = import_id
    audit.record(organization_id, actor_user_id, f"{action}.updated" if link_id else f"{action}.created", item, metadata)
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

def process_import_batch(organization_id, actor_user_id, kind, rows, metadata, is_authoritative=False):
    from app.models.uip import UipRegisterImport, UipRegisterImportException, UipPropertyMember, UipProperty, UipMemberProfile
    from app.extensions import db
    
    batch = UipRegisterImport(
        organization_id=organization_id,
        source_type="MUNICIPAL",
        source_identifier=metadata.get("source_identifier") or "MASTER_ROLL",
        batch_reference=metadata.get("batch_reference") or "MASTER",
        date_received=metadata.get("date_received"),
        effective_date=metadata.get("effective_date"),
        imported_by_user_id=actor_user_id,
        document_id=metadata.get("document_id"),
        notes="master_roll",
        status="PROCESSING"
    )
    db.session.add(batch)
    db.session.flush()
    
    summary = {"created": 0, "updated": 0, "exceptions": 0}
    

    for idx, raw_row in enumerate(rows, 2):
        try:
            # Strip whitespace from dictionary keys to forgive messy CSV headers
            row = {k.strip(): v for k, v in raw_row.items() if k is not None}
            
            mem_ref = str(row.get("member_reference") or "").strip()
            prop_ref = str(row.get("property_reference") or "").strip()
            
            if not mem_ref or not prop_ref:
                raise Exception("Missing mandatory references (member_reference or property_reference)")
                
            # Raw Insert/Update Member
            member = UipMemberProfile.query.filter_by(organization_id=organization_id, reference=mem_ref).first()
            if not member:
                member = UipMemberProfile(organization_id=organization_id, reference=mem_ref)
                db.session.add(member)
                summary["created"] += 1
            else:
                summary["updated"] += 1
                
            member.name = (row.get("name") or "").strip()
            member.email = (row.get("email") or "").strip()
            member.phone = (row.get("phone") or "").strip()
            member.member_type = (row.get("member_type") or "person").strip().lower()
            member.record_source = "MUNICIPAL"
            member.is_active = True
            member.last_import_id = batch.id
            
            # Raw Insert/Update Property
            prop = UipProperty.query.filter_by(organization_id=organization_id, reference=prop_ref).first()
            if not prop:
                prop = UipProperty(organization_id=organization_id, reference=prop_ref)
                db.session.add(prop)
            prop.address = (row.get("address") or "").strip()
            prop.rates_reference = (row.get("rates_reference") or "").strip()
            prop.classification = (row.get("classification") or "Residential").strip()
            prop.is_active = True
            prop.last_import_id = batch.id

            
            db.session.flush()
            
            # Raw Insert/Update Link
            link = UipPropertyMember.query.filter_by(organization_id=organization_id, member_id=member.id, property_id=prop.id, valid_to=None).first()
            if not link:
                link = UipPropertyMember(organization_id=organization_id, member_id=member.id, property_id=prop.id)
                db.session.add(link)
                link.relationship = "owner"
                link.valid_from = batch.effective_date
                link.is_verified = True
                link.last_import_id = batch.id
            
        except Exception as e:
            db.session.add(UipRegisterImportException(
                import_id=batch.id,
                row_number=idx,
                source_reference=row.get("member_reference") or "unknown",
                reason=str(e),
                incoming_data=row
            ))
            summary["exceptions"] += 1
            
    batch.status = "COMPLETED" if summary["exceptions"] == 0 else "WITH_EXCEPTIONS"
    db.session.flush()
    return batch, summary






















