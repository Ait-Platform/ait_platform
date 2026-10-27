"""Read-only PM property -> lease tenant register. No shared debtor/job-card access."""
from sqlalchemy import text
from flask_login import current_user
from app.extensions import db
from .lease_certificate import selected_tenants
from .tenant_reference import read as tenant_reference


def tenants_for(property_id):
    # An issued PM lease is a relationship even when the tenant register property is blank.
    identifiers={row[0]:None for row in db.session.execute(text('''SELECT DISTINCT l.tenant_id
        FROM bil_lease l JOIN bil_property p ON p.id=l.property_id
        WHERE p.id=:p AND p.manager_id=:u AND p.is_archived=false'''),dict(p=property_id,u=current_user.id))}
    # Lease Management selects its most recently saved working draft for this property.
    draft=db.session.execute(text('''SELECT d.id,d.items FROM bil_lease_draft d
        JOIN bil_property p ON p.id=d.property_id
        WHERE d.property_id=:p AND d.manager_id=:u AND p.manager_id=:u
        AND p.is_archived=false AND d.status='draft' ORDER BY d.updated_at DESC,d.id DESC LIMIT 1'''),dict(p=property_id,u=current_user.id)).mappings().first()
    if draft:
        for party in selected_tenants(draft['items']):
            value=party.get('tenant_id')
            if str(value or '').isdigit():identifiers[int(value)]=str(draft['id'])
    if not identifiers:return []
    result=[]
    for row in db.session.execute(text('''SELECT id,name,email,phone,notes FROM bil_tenant
        WHERE manager_id=:u AND id=ANY(:ids) ORDER BY name,id'''),dict(u=current_user.id,ids=list(identifiers))).mappings():
        entry=dict(row);entry['tenant_reference']=tenant_reference(entry.pop('notes'));entry['draft_id']=identifiers[row['id']];result.append(entry)
    return result
