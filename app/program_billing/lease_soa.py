"""Read-only PM property -> lease tenant register. No shared debtor/job-card access."""
from sqlalchemy import text,inspect
from flask_login import current_user
from app.extensions import db
from .lease_certificate import selected_tenants
from .tenant_reference import read as tenant_reference


def tenants_for(property_id):
    # Inspect this connection, not ORM declarations: legacy production uses unit links.
    catalog=inspect(db.session.connection())
    def columns(table):
        return {row['name'] for row in catalog.get_columns(table)} if catalog.has_table(table) else set()
    lease_columns=columns('bil_lease');tenant_columns=columns('bil_tenant')
    relation=None
    if 'property_id' in lease_columns:
        relation='JOIN bil_property p ON p.id=l.property_id'
    elif 'sectional_unit_id' in lease_columns and {'id','property_id'}<=columns('bil_sectional_unit'):
        relation='JOIN bil_sectional_unit unit ON unit.id=l.sectional_unit_id JOIN bil_property p ON p.id=unit.property_id'
    elif 'property_id' in tenant_columns:
        relation='JOIN bil_tenant linked_tenant ON linked_tenant.id=l.tenant_id JOIN bil_property p ON p.id=linked_tenant.property_id'
    identifiers={}
    if relation and 'tenant_id' in lease_columns:
        identifiers={row[0]:None for row in db.session.execute(text('SELECT DISTINCT l.tenant_id FROM bil_lease l '+relation+
            ' WHERE p.id=:p AND p.manager_id=:u AND p.is_archived=false'),dict(p=property_id,u=current_user.id))}
    draft=None
    if {'id','items','property_id','manager_id','status','updated_at'}<=columns('bil_lease_draft'):
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
    # Legacy tenant rows may predate manager_id; their property scope was verified above.
    manager_scope=' AND manager_id=:u' if 'manager_id' in tenant_columns else ''
    for row in db.session.execute(text('SELECT id,name,email,phone,notes FROM bil_tenant WHERE id=ANY(:ids)'+manager_scope+' ORDER BY name,id'),dict(u=current_user.id,ids=list(identifiers))).mappings():
        entry=dict(row);entry['tenant_reference']=tenant_reference(entry.pop('notes'));entry['draft_id']=identifiers[row['id']];result.append(entry)
    return result
