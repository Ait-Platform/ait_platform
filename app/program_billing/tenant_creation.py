"""Construct new PM tenants from explicit, authorised property/unit records."""
from flask import abort
from flask_login import current_user
from app.models.billing import BilTenant


def tenant_for_unit(property_record, unit_record, **fields):
    if not current_user.is_authenticated or property_record.manager_id != current_user.id:
        abort(403)
    if (not property_record.id or not unit_record.id or
            unit_record.property_id != property_record.id):
        abort(400, description="A unit belonging to the selected property is required.")
    if any(key in fields for key in ("property_id", "sectional_unit_id", "is_active")):
        abort(400, description="Tenant relationships and activity cannot be inferred.")
    return BilTenant(property_id=property_record.id, sectional_unit_id=unit_record.id,
        is_active=None, **fields)
