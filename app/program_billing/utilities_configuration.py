"""Property-scoped, non-destructive METSOA configuration editing.
Legacy unassociated rows are never claimed. No financial/history rows are changed.
"""
import hashlib,json
from flask import abort
from sqlalchemy import text,inspect
from app.extensions import db
from .metsoa_setup_safety import property_scope

def rows(table,property_id):
    return [dict(r) for r in db.session.execute(text('SELECT * FROM '+table+' WHERE property_id=:p ORDER BY id'),dict(p=property_id)).mappings()]

def revision(property_id):
    content=[rows('bil_muni_account',property_id),rows('bil_meter',property_id)]
    return hashlib.sha256(json.dumps(content,sort_keys=True,default=str).encode()).hexdigest()

def save(property_id,data):
    property_id=property_scope(property_id,lock=True)
    if not isinstance(data,dict) or data.get('configuration_revision')!=revision(property_id):
        abort(409,description='Configuration changed or the editor is outdated. Reopen Setup before saving; no records were changed.')
    accounts=data.get('accounts');groups=('bulkWater','bulkElec','subWater','subElec')
    if not isinstance(accounts,list) or not accounts:abort(400,description='Enter at least one municipal account.')
    if any(not isinstance(data.get(k,[]),list) for k in groups):abort(400)
    old_accounts={r['id']:r for r in rows('bil_muni_account',property_id)}
    old_meters={r['id']:r for r in rows('bil_meter',property_id)}
    account_ids=set();meter_ids=set();numbers=set();meter_numbers=set();account_map={};meter_map={}
    # Identity locks also prevent concurrent new setups claiming the same number.
    identities=[]
    for kind,entries in [('account',accounts)]+[('meter',data.get(k,[])) for k in groups]:
        for item in entries:
            if not isinstance(item,dict):abort(400)
            number=str(item.get('number') or '').strip()
            if not number or len(number)>50:abort(400,description='Account and meter numbers must contain 1–50 characters.')
            identities.append(kind+':'+number)
    for identity in sorted(set(identities)):
        lock=int.from_bytes(hashlib.sha256(('METSOA_SETUP:'+identity).encode()).digest()[:8],'big',signed=True)
        db.session.execute(text('SELECT pg_advisory_xact_lock(:key)'),dict(key=lock))
    for a in accounts:
        key=a.get('id');number=str(a['number']).strip();record=a.get('record_id')
        if not key or key in account_map or number in numbers:abort(400,description='Duplicate account entry.')
        numbers.add(number)
        if record:
            if record not in old_accounts or record in account_ids:abort(403)
            account_ids.add(record)
            # Renumbering an account would invalidate historical account-number links.
            if number!=old_accounts[record]['account_number']:abort(409,description='Existing account numbers are protected; contact support to correct an historical identity.')
        elif db.session.execute(text('SELECT 1 FROM bil_muni_account WHERE account_number=:n LIMIT 1'),dict(n=number)).first():
            abort(409,description='This account number already exists. It cannot be reassigned or duplicated through Setup.')
        account_map[key]=dict(item=a,record=record,number=number)
    if account_ids!=set(old_accounts):abort(409,description='Existing accounts cannot be removed through Setup.')
    mapping={}
    for entry in data.get('mapping',[]):
        if entry.get('account_id') not in account_map:abort(400,description='Unknown municipal account in mapping.')
        for kind in ('water','elec'):
            key=entry.get(kind)
            if key:
                if key in mapping:abort(400,description='A meter is assigned more than once.')
                mapping[key]=entry['account_id']
    bulk_accounts=[key for key,a in account_map.items() if a['item'].get('isBulk')]
    for group in groups:
        for m in data.get(group,[]):
            key=m.get('id');number=str(m['number']).strip();record=m.get('record_id')
            if not key or key in meter_map or number in meter_numbers:abort(400,description='Duplicate meter entry.')
            meter_numbers.add(number)
            if record:
                if record not in old_meters or record in meter_ids:abort(403)
                meter_ids.add(record)
                if number!=old_meters[record]['meter_number']:abort(409,description='Existing meter numbers are protected to preserve reading history.')
            elif db.session.execute(text('SELECT 1 FROM bil_meter WHERE meter_number=:n LIMIT 1'),dict(n=number)).first():
                abort(409,description='This meter number already exists. It cannot be reassigned or duplicated through Setup.')
            account=m.get('account_id') or (bulk_accounts[0] if group.startswith('bulk') and len(bulk_accounts)==1 else mapping.get(key))
            if not account and len(account_map)==1:account=next(iter(account_map))
            if account not in account_map:abort(400,description='Each meter requires its municipal account.')
            utility='Water' if group.endswith('Water') else 'Electrical'
            if record:
                previous=old_meters[record]
                if previous['municipal_bill_number']!=account_map[account]['number'] or ('water' in (previous['utility_type'] or '').lower())!=(utility=='Water'):
                    abort(409,description='Existing meter account and type are protected to preserve historical billing.')
            meter_map[key]=dict(item=m,record=record,number=number,account=account,utility=utility,group=group)
    if meter_ids!=set(old_meters):abort(409,description='Existing meters cannot be removed through Setup.')
    # Do not manufacture a legacy sectional unit to satisfy old NOT NULL constraints.
    if any(not m['record'] for m in meter_map.values()):
        columns=inspect(db.session.connection()).get_columns('bil_meter')
        if any(c['name']=='sectional_unit_id' and not c['nullable'] for c in columns):
            abort(503,description='Legacy bil_meter.sectional_unit_id is mandatory. A reviewed nullability migration is required before creating property-based meters; no records were changed.')
    # Existing configuration edits must not write initial readings, financial particulars or exceptions.
    # They retain their existing SQL records unchanged.
    owners={o.get('account_id'):o for o in data.get('owners',[]) if isinstance(o,dict)}
    for key,a in account_map.items():
        owner=owners.get(key,{})
        name=str(owner.get('name') or '').strip()
        if len(name)>100:abort(400,description='Owner name is too long.')
        owner_id=None
        if name:
            # Shared owner names are immutable; choose/create, never rename another account's owner.
            db.session.execute(text('INSERT INTO ref_muni_owner(name) VALUES(:n) ON CONFLICT(name) DO NOTHING'),dict(n=name))
            owner_id=db.session.execute(text('SELECT id FROM ref_muni_owner WHERE name=:n'),dict(n=name)).scalar()
        values=dict(p=property_id,n=a['number'],b=bool(a['item'].get('isBulk')),o=owner_id,e=owner.get('email'),address=owner.get('address'))
        if a['record']:
            values['id']=a['record']
            db.session.execute(text('UPDATE bil_muni_account SET is_bulk_account=:b,owner_id=COALESCE(:o,owner_id),muni_email=COALESCE(:e,muni_email),owner_address=COALESCE(:address,owner_address) WHERE id=:id AND property_id=:p'),values)
        else:
            a['record']=db.session.execute(text('INSERT INTO bil_muni_account(property_id,account_number,is_bulk_account,owner_id,muni_email,owner_address) VALUES(:p,:n,:b,:o,:e,:address) RETURNING id'),values).scalar()
    for m in meter_map.values():
        if not m['record']:
            m['record']=db.session.execute(text("INSERT INTO bil_meter(property_id,meter_number,utility_type,municipal_bill_number,status) VALUES(:p,:n,:u,:a,'active') RETURNING id"),dict(p=property_id,n=m['number'],u=m['utility'],a=account_map[m['account']]['number'])).scalar()
    # Bulk relationships are applied only after every referenced meter ID is known.
    for key,m in meter_map.items():
        parent=m['item'].get('parent_id')
        if parent and parent not in meter_map:abort(400,description='Unknown supplying bulk meter.')
        if parent and (parent==key or not meter_map[parent]['group'].startswith('bulk') or meter_map[parent]['utility']!=m['utility']):
            abort(400,description='Invalid supplying bulk meter.')
        seen={key};cursor=parent
        while cursor:
            if cursor in seen:abort(400,description='Circular bulk-meter relationship.')
            seen.add(cursor);cursor=meter_map[cursor]['item'].get('parent_id')
            if cursor and cursor not in meter_map:abort(400)
        if m['record'] in old_meters and old_meters[m['record']]['parent_meter_id'] and not parent:
            # Never silently clear a historical relationship omitted by the old UI.
            continue
        db.session.execute(text('UPDATE bil_meter SET parent_meter_id=:parent WHERE id=:id AND property_id=:p'),dict(parent=meter_map[parent]['record'] if parent else None,id=m['record'],p=property_id))
    db.session.flush()
    return dict(success=True,message='Utilities configuration saved. Existing accounts, meters and history were preserved.')
