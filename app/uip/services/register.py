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
    from app.models.core import CoreRoleAssignment, CoreRole
    from app.models.uip_governance import UipDelegation
    
    is_manager = CoreRoleAssignment.query.join(CoreRole).filter(
        CoreRoleAssignment.organization_id == organization_id,
        CoreRoleAssignment.user_id == actor_user_id,
        CoreRole.slug == "manager"
    ).first()
    if is_manager:
        return True
        
    delegation = UipDelegation.query.filter_by(
        organization_id=organization_id,
        delegated_user_id=actor_user_id,
        delegation_type="RATEPAYER_ADMIN",
        status="ACTIVE"
    ).first()
    if delegation:
        return True
        
    abort(403, description="Register administration authority required.")

def available_memberships(organization_id, actor_user_id):
    require_register_admin(organization_id, actor_user_id)
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


def choice(data, key, values):
    value = data.get(key)
    if value not in values:
        raise InvalidRegisterOption(key, value, values)
    return value


def boolean(data, key):
    return choice(data, key, ("true", "false")) == "true"


def save_member(organization_id, actor_user_id, data, member_id=None, is_import=False, import_id=None):
    require_register_admin(organization_id, actor_user_id)
    if not is_import and member_id is None:
        abort(403, description="Manual creation of authoritative ratepayers is prohibited.")
        
    values = {
        "reference": text(data, "reference", 50, True), "name": text(data, "name", 255, True),
        "member_type": choice(data, "member_type", ("person", "business")),
        "email": text(data, "email", 255), "phone": text(data, "phone", 50),
        "is_active": boolean(data, "is_active"),
        "eligibility_status": choice(data, "eligibility_status", ("unverified", "eligible", "ineligible")),
    }
    
    if is_import and member_id is not None:
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
        member = UipMemberProfile(organization_id=organization_id, membership_id=membership.id, record_source="MUNICIPAL" if is_import else "MANUAL")
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
                    
        if is_import:
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


def save_property(organization_id, actor_user_id, data, property_id=None, is_import=False, import_id=None):
    require_register_admin(organization_id, actor_user_id)
    if not is_import and property_id is None:
        abort(403, description="Manual creation of authoritative properties is prohibited.")
        
    values = {
        "reference": text(data, "reference", 50, True), "address": text(data, "address", 500, True),
        "rates_reference": text(data, "rates_reference", 100),
        "classification": choice(data, "classification", ("residential", "business", "mixed", "other")),
        "is_active": boolean(data, "is_active"),
    }
    
    if property_id:
        item = get(UipProperty, organization_id, actor_user_id, property_id)
        if not is_import and getattr(item, 'record_source', 'MANUAL') == 'MUNICIPAL':
            for sealed in ("reference", "address", "rates_reference", "classification"):
                if sealed in values and getattr(item, sealed) != values[sealed]:
                    abort(403, description=f"Cannot manually modify sealed municipal field '{sealed}'.")
    else:
        item = UipProperty(organization_id=organization_id, record_source="MUNICIPAL" if is_import else "MANUAL")
        db.session.add(item)
        
    if is_import:
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


def save_relationship(organization_id, actor_user_id, data, property_id=None, member_id=None, link_id=None, is_import=False, import_id=None):
    require_register_admin(organization_id, actor_user_id)
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
        item = model(organization_id=organization_id, record_source="MUNICIPAL" if is_import else "MANUAL", **values)
        db.session.add(item)
        
    if is_import:
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
def process_import_batch(organization_id, actor_user_id, kind, rows, metadata):
    from app.models.uip import UipRegisterImport, UipRegisterImportException, UipPropertyMember, UipProperty, UipMemberProfile
    
    # 1. Create the Import record
    batch = UipRegisterImport(
        organization_id=organization_id,
        source_type="MUNICIPAL",
        source_identifier=metadata.get("source_identifier"),
        batch_reference=metadata.get("batch_reference"),
        date_received=metadata.get("date_received"),
        effective_date=metadata.get("effective_date"),
        imported_by_user_id=actor_user_id,
        document_id=metadata.get("document_id"),
        status="PROCESSING"
    )
    db.session.add(batch)
    db.session.flush()

    summary = {"received": len(rows), "created": 0, "updated": 0, "unchanged": 0, "exceptions": 0}
    
    for idx, row in enumerate(rows, 2):
        try:
            if kind == "members":
                reference = row.get("reference")
                if not reference:
                    raise InvalidRegisterOption("reference", "Missing municipal reference", [])
                    
                existing = UipMemberProfile.query.filter_by(organization_id=organization_id, reference=reference).first()
                if existing:
                    if existing.record_source == "MANUAL":
                        # Upgrade
                        save_member(organization_id, actor_user_id, row, existing.id, is_import=True, import_id=batch.id)
                        summary["updated"] += 1
                    else:
                        # Update
                        changed = False
                        for f in ["name", "member_type"]:
                            if getattr(existing, f) != row.get(f):
                                changed = True
                        if changed or existing.is_active != (row.get("is_active") == "true"):
                            save_member(organization_id, actor_user_id, row, existing.id, is_import=True, import_id=batch.id)
                            summary["updated"] += 1
                        else:
                            summary["unchanged"] += 1
                else:
                    save_member(organization_id, actor_user_id, row, None, is_import=True, import_id=batch.id)
                    summary["created"] += 1
                    
            elif kind == "properties":
                reference = row.get("reference")
                if not reference:
                    raise InvalidRegisterOption("reference", "Missing municipal reference", [])
                    
                existing = UipProperty.query.filter_by(organization_id=organization_id, reference=reference).first()
                if existing:
                    if existing.record_source == "MANUAL":
                        save_property(organization_id, actor_user_id, row, existing.id, is_import=True, import_id=batch.id)
                        summary["updated"] += 1
                    else:
                        changed = False
                        for f in ["address", "rates_reference", "classification"]:
                            if getattr(existing, f) != row.get(f):
                                changed = True
                        if changed or existing.is_active != (row.get("is_active") == "true"):
                            save_property(organization_id, actor_user_id, row, existing.id, is_import=True, import_id=batch.id)
                            summary["updated"] += 1
                        else:
                            summary["unchanged"] += 1
                else:
                    save_property(organization_id, actor_user_id, row, None, is_import=True, import_id=batch.id)
                    summary["created"] += 1
                    
            elif kind == "relationships":
                member_ref = row.get("member_reference")
                prop_ref = row.get("property_reference")
                rel_type = row.get("relationship")
                
                member = UipMemberProfile.query.filter_by(organization_id=organization_id, reference=member_ref).first()
                prop = UipProperty.query.filter_by(organization_id=organization_id, reference=prop_ref).first()
                
                if not member or not prop:
                    raise Exception("Referenced member or property not found in register.")
                    
                # Close existing active relationships for this property
                active_rels = UipPropertyMember.query.filter_by(
                    organization_id=organization_id, property_id=prop.id
                ).filter(UipPropertyMember.valid_to.is_(None)).all()
                
                already_active = False
                for active_rel in active_rels:
                    if active_rel.member_id == member.id and active_rel.relationship == rel_type:
                        already_active = True
                        continue
                    # Close the previous relationship as at the effective_date
                    active_rel.valid_to = batch.effective_date
                    audit.record(organization_id, actor_user_id, "ownership.updated", active_rel, {"changed_fields": ["valid_to"], "import_id": batch.id})
                    
                if not already_active:
                    # Create the new active relationship
                    row["valid_from"] = batch.effective_date.strftime("%Y-%m-%d")
                    row["valid_to"] = ""
                    row["member_id"] = member.id
                    save_relationship(organization_id, actor_user_id, row, property_id=prop.id, member_id=member.id, is_import=True, import_id=batch.id)
                    summary["created"] += 1
                else:
                    summary["unchanged"] += 1

        except Exception as e:
            db.session.add(UipRegisterImportException(
                import_id=batch.id,
                row_number=idx,
                source_reference=row.get("reference") or row.get("member_reference"),
                reason=str(e),
                incoming_data=row
            ))
            summary["exceptions"] += 1
            
    batch.status = "COMPLETED" if summary["exceptions"] == 0 else "WITH_EXCEPTIONS"
    db.session.flush()
    return batch, summary

