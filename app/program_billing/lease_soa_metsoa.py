"""Transfer saved METSOA meter/month totals to PM; never calculate tariffs or mutate sources."""
import hashlib
import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from flask import abort
from flask_login import current_user
from itsdangerous import BadSignature
from sqlalchemy import inspect, text
from . import lease_soa_posting as posting
from .lease_soa_dates import today

TYPES={'electricity':('electricity','Electricity'),'electrical':('electricity','Electricity'),
    'water':('water','Water / Sewerage'),'sanitation':('sewerage','Sewerage'),
    'sewerage':('sewerage','Sewerage'),'refuse':('refuse','Refuse Removal'),
    'other_utilities':('other_utilities','Other Utilities')}


def require_sources():
    posting.require_posting_schema()
    catalog=inspect(posting.db.session.connection())
    for table,required in [('bil_metsoa_meter_month',{'id','tenant_id','meter_id','month','utility_type','total_due'}),('bil_meter',{'id','property_id'})]:
        if not catalog.has_table(table) or not required<={c['name'] for c in catalog.get_columns(table)}:
            abort(503,description='Saved METSOA meter/month totals and meter property links are required. No utility charge was posted.')


def key_for(row):
    # Natural source identity survives a regenerated METSOA row; never double transfer.
    return hashlib.sha256(f"PMMETSOA:{row['tenant_id']}:{row['meter_id']}:{row['month']}".encode()).hexdigest()[:32]


def source_rows(property_id,tenant_id,source_id=None,lock=False):
    require_sources();posting.authorise(property_id,tenant_id)
    sql="""SELECT s.*,m.property_id FROM bil_metsoa_meter_month s
        JOIN bil_meter m ON m.id=s.meter_id
        JOIN bil_property p ON p.id=m.property_id
        WHERE s.tenant_id=:t AND m.property_id=:p AND p.manager_id=:m AND NOT p.is_archived"""
    params=dict(t=tenant_id,p=property_id,m=current_user.id)
    if source_id is not None:sql+=' AND s.id=:id';params['id']=source_id
    sql+=' ORDER BY s.month,s.id'
    if lock:sql+=' FOR SHARE OF s,m'
    return [dict(r) for r in posting.db.session.execute(text(sql),params).mappings()]


def describe(row,property_id,tenant_id):
    category,label=TYPES.get(str(row['utility_type'] or '').lower(),(None,row['utility_type'] or 'Unknown utility'))
    reason=None
    try:
        if not re.fullmatch(r'\d{4}-\d{2}',row['month']):raise ValueError()
        period=date.fromisoformat(row['month']+'-01')
        amount=Decimal(str(row['total_due']))
        if not amount.is_finite() or amount<=0 or amount>Decimal('99999999.99'):raise ValueError()
        amount=amount.quantize(Decimal('.01'))
        if amount<=0:raise ValueError()
        if period>today():reason='Future billing period'
        current=row.get('curr_date')
        if current:
            try:
                if date.fromisoformat(str(current).replace('/','-')[:10])>today():reason='Future meter reading'
            except ValueError:reason='Invalid saved reading date'
    except (ValueError,InvalidOperation,TypeError):
        amount=None;reason='Invalid saved amount or billing period'
    if category is None:reason='Unsupported saved utility type'
    snapshot=dict(source='bil_metsoa_meter_month',source_id=row['id'],tenant_id=tenant_id,
        property_id=property_id,meter_id=row['meter_id'],period=row['month'],utility=category,
        saved_total=str(row['total_due']),reading_date=str(row.get('curr_date') or ''))
    digest=hashlib.sha256(json.dumps(snapshot,sort_keys=True).encode()).hexdigest()
    key=key_for(row)
    posted=posting.db.session.execute(text('SELECT tenant_id,ref,posting_hash FROM bil_tenant_ledger WHERE posting_key=:key'),dict(key=key)).mappings().first()
    if posted:
        if posted['tenant_id']!=tenant_id:abort(409,description='The METSOA source identity conflicts with an existing ledger entry.')
        status='Posted' if posted['posting_hash']==digest else 'Posted — source subsequently changed; use a controlled correction'
        reason='Already posted'
    else:
        status=reason or 'Ready'
        # Legacy aggregate postings cannot safely be split/reposted by meter.
        legacy=posting.db.session.execute(text("""SELECT ref FROM bil_tenant_ledger WHERE tenant_id=:t AND month=:month
            AND (ref IN ('METSOA-AUTO',:old_ref) OR description LIKE 'Due to Metro%') LIMIT 1"""),
            dict(t=tenant_id,month=row['month'],old_ref='METSOA '+row['month'])).scalar()
        scheduled=posting.db.session.execute(text("""SELECT ref FROM bil_tenant_ledger WHERE tenant_id=:t AND month=:month
            AND rent_property_id=:p AND description=ANY(:descriptions) AND rent_schedule_id IS NOT NULL LIMIT 1"""),
            dict(t=tenant_id,p=property_id,month=row['month'],descriptions=['Water','Sewerage'] if category=='water' else [label])).scalar() if 'rent_schedule_id' in {c['name'] for c in inspect(posting.db.session.connection()).get_columns('bil_tenant_ledger')} else None
        if legacy or scheduled:reason='Existing utility posting requires reconciliation';status=reason
    return dict(id=row['id'],period=row['month'],label=label,amount=amount,status=status,
        ref=posted['ref'] if posted else None,can_post=reason is None,key=key,digest=digest,snapshot=snapshot,
        token=posting.token(property_id,tenant_id,'metsoa:'+str(row['id'])+':'+digest))


def list_charges(property_id,tenant_id):
    return [describe(r,property_id,tenant_id) for r in source_rows(property_id,tenant_id)]


def post(property_id,tenant_id,form):
    try:source_id=int(form.get('source_id',''))
    except (ValueError,TypeError):abort(400,description='Select a saved METSOA charge.')
    rows=source_rows(property_id,tenant_id,source_id,lock=True)
    if len(rows)!=1:abort(404)
    row=rows[0]
    lock=int.from_bytes(hashlib.sha256(f"PMUTILITY:{tenant_id}:{row['month']}".encode()).digest()[:8],'big',signed=True)
    posting.db.session.execute(text('SELECT pg_advisory_xact_lock(:key)'),dict(key=lock))
    charge=describe(row,property_id,tenant_id)
    try:
        scope=posting.signer().loads(form.get('posting_key',''))
        if not isinstance(scope,list) or len(scope)!=5 or scope[:4]!=[current_user.id,property_id,tenant_id,'metsoa:'+str(source_id)+':'+charge['digest']]:raise BadSignature('scope')
    except (BadSignature,TypeError):abort(409,description='The saved METSOA charge changed or this form is invalid. Reopen Utilities.')
    if charge['ref']:
        if 'subsequently changed' in charge['status']:abort(409,description=charge['status'])
        posting.db.session.commit();return False
    if not charge['can_post']:abort(409,description=charge['status'])
    audit=dict(charge['snapshot'],metsoa_reference='bil_metsoa_meter_month:'+str(source_id),
        recorded_by=current_user.id,recorded_at=datetime.now(timezone.utc).isoformat())
    # The charge month is the source billing period; its effective date is the transfer date.
    entry_id=posting.append_entry(tenant_id,today(),charge['label'],'charge',charge['amount'],charge['key'],charge['digest'],
        'PMMETSOA:'+json.dumps(audit,sort_keys=True))
    posting.db.session.execute(text('UPDATE bil_tenant_ledger SET month=:month WHERE id=:id'),dict(month=charge['period'],id=entry_id))
    posting.db.session.commit();return True
