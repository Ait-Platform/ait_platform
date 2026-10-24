"""Stable payment references in reserved tenant notes metadata; free-form notes survive."""
import re,uuid
from sqlalchemy import text
PATTERN=re.compile(r'(?m)^\[AIT Tenant Reference: (TR-[A-F0-9]{16})\]$')

def read(notes):
    match=PATTERN.search(notes or '')
    return match.group(1) if match else ''

def marker(reference):
    if not re.fullmatch(r'TR-[A-F0-9]{16}',reference or ''):raise ValueError('Invalid Tenant Reference.')
    return '[AIT Tenant Reference: '+reference+']'

def lock_allocation(session):
    session.execute(text("SELECT pg_advisory_xact_lock(hashtext('ait.tenant.payment-reference'))"))

def ensure(session,tenant_id,manager_id):
    lock_allocation(session)
    row=session.execute(text('SELECT notes FROM bil_tenant WHERE id=:t AND manager_id=:u FOR UPDATE'),{'t':tenant_id,'u':manager_id}).mappings().one()
    reference=read(row['notes'])
    if reference:return reference
    while True:
        reference='TR-'+uuid.uuid4().hex[:16].upper()
        exists=session.execute(text("SELECT 1 FROM bil_tenant WHERE strpos(coalesce(notes,''),:marker)>0 LIMIT 1"),{'marker':marker(reference)}).first()
        if not exists:break
    notes=row['notes'] or ''
    session.execute(text('UPDATE bil_tenant SET notes=:notes WHERE id=:t AND manager_id=:u'),{'notes':notes+('\n' if notes and not notes.endswith('\n') else '')+marker(reference),'t':tenant_id,'u':manager_id})
    return reference

def ensure_for_manager(session,manager_id):
    """Complete legacy references during an authorised write, never a GET request."""
    lock_allocation(session)
    records=session.execute(text('SELECT id,notes FROM bil_tenant WHERE manager_id=:u ORDER BY id FOR UPDATE'),{'u':manager_id}).mappings().all()
    for record in records:
        if not read(record['notes']):ensure(session,record['id'],manager_id)


def resolve(session,manager_id,reference):
    """Read-only, manager-scoped lookup for rental allocation and reconciliation."""
    tag=marker(reference)
    return session.execute(text("SELECT id,name,manager_id FROM bil_tenant WHERE manager_id=:u AND strpos(coalesce(notes,''),:marker)>0"),{'u':manager_id,'marker':tag}).mappings().one_or_none()
