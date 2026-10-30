"""Explicit municipal account association. Never rewrites meter or financial history."""
import hashlib,json
from flask import abort,current_app
from flask_login import current_user
from itsdangerous import URLSafeTimedSerializer,BadSignature,SignatureExpired
from sqlalchemy import text,inspect
from app.extensions import db
from .metsoa_setup_safety import property_scope

def state(property_id,account_id,lock=False):
    account=db.session.execute(text('SELECT * FROM bil_muni_account WHERE id=:a'+(' FOR UPDATE' if lock else '')),dict(a=account_id)).mappings().first()
    if not account:abort(404)
    account=dict(account);conflicts=[];evidence=[]
    if account['property_id'] is not None:conflicts.append('Account is already associated with a property; it cannot be reassigned.')
    if account.get('is_bulk_account'):conflicts.append('Bulk municipal accounts require a separate reviewed association process.')
    number=account.get('account_number') or ''
    duplicate=db.session.execute(text('SELECT id FROM bil_muni_account WHERE id<>:a AND lower(btrim(account_number))=lower(btrim(:n)) ORDER BY id'),dict(a=account_id,n=number)).scalars().all()
    if duplicate:conflicts.append('Another municipal account uses this number. Resolve duplicate account records before association.')
    pointers=[account[k] for k in ('water_meter_id','elec_meter_id') if account.get(k)]
    declared=[account[k] for k in ('muni_water_meter_no','muni_elec_meter_no') if account.get(k)]
    meters=[dict(r) for r in db.session.execute(text("SELECT * FROM bil_meter WHERE id=ANY(:ids) OR (NULLIF(btrim(:n),'') IS NOT NULL AND municipal_bill_number=:n) OR meter_number=ANY(:declared) ORDER BY id"+(' FOR UPDATE' if lock else '')),dict(ids=pointers,n=number,declared=declared)).mappings()]
    ids=[m['id'] for m in meters]
    if set(pointers)-set(ids):conflicts.append('An account meter pointer refers to a missing record.')
    catalog=inspect(db.session.connection())
    def historical_scope(record,label):
        prop=record.get('property_id')
        if prop not in (None,'',str(property_id),property_id):conflicts.append(label+' has a conflicting property association.')
        unit=record.get('sectional_unit_id')
        if unit not in (None,''):
            if not catalog.has_table('bil_sectional_unit'):
                conflicts.append(label+' has an unresolved historical sectional-unit reference.');return
            row=db.session.execute(text('SELECT property_id FROM bil_sectional_unit WHERE id::text=:u'),dict(u=str(unit))).mappings().first()
            evidence.append(dict(kind='historical_unit',id=unit,property_id=row['property_id'] if row else None))
            if not row or row['property_id']!=property_id:conflicts.append(label+' has an unresolved or conflicting historical sectional-unit property.')
    for meter in meters:
        historical_scope(meter,'Meter')
        if meter.get('municipal_bill_number') not in (None,'',number):conflicts.append('A meter pointer or declared meter number conflicts with its existing municipal account number.')
        if meter.get('parent_meter_id') or db.session.execute(text('SELECT 1 FROM bil_meter WHERE parent_meter_id=:m LIMIT 1'),dict(m=meter['id'])).first():conflicts.append('A meter participates in a bulk relationship. Association is blocked pending a separate review.')
        if meter.get('replacement_for_meter_id') or db.session.execute(text('SELECT 1 FROM bil_meter WHERE replacement_for_meter_id=:m LIMIT 1'),dict(m=meter['id'])).first():conflicts.append('Meter replacement relationships require separate verification before association.')
        if db.session.execute(text('SELECT 1 FROM bil_muni_account WHERE id<>:a AND (water_meter_id=:m OR elec_meter_id=:m) LIMIT 1'),dict(a=account_id,m=meter['id'])).first():conflicts.append('A related meter is referenced by another municipal account.')
        if db.session.execute(text('SELECT 1 FROM bil_meter WHERE id<>:m AND lower(btrim(meter_number))=lower(btrim(:n)) LIMIT 1'),dict(m=meter['id'],n=meter['meter_number'])).first():conflicts.append('A related meter identifier has duplicate records.')
        utility=(meter.get('utility_type') or '').lower()
        if utility not in ('water','electricity','electrical'):conflicts.append('A related meter has an unsupported or missing utility type.')
    for kind,pointer,declared_field,types in [('water','water_meter_id','muni_water_meter_no',['water']),('electricity','elec_meter_id','muni_elec_meter_no',['electricity','electrical'])]:
        group=[m for m in meters if (m.get('utility_type') or '').lower() in types]
        if len(group)>1:conflicts.append('Multiple '+kind+' meters require manual resolution before association on this page.')
        if account.get(pointer):
            match=next((m for m in meters if m['id']==account[pointer]),None)
            if match and (match.get('utility_type') or '').lower() not in types:conflicts.append('An existing account meter pointer has the wrong utility type.')
            if match and account.get(declared_field) and account[declared_field]!=match['meter_number']:conflicts.append('The declared meter number and saved meter pointer disagree.')
    # Inspect explicit historical relationships only; amounts/readings are never updated.
    for table in ('bil_meter_reading','bil_consumption','bil_metro_reading_log','bil_muni_cycle_totals','bil_metsoa_cycle','bil_muni_payment','bil_municipal_payment'):
        if not catalog.has_table(table):continue
        columns={c['name'] for c in catalog.get_columns(table)}
        clauses=[];params=dict(ids=ids,a=account_id,n=number)
        if 'meter_id' in columns:clauses.append('h.meter_id=ANY(:ids)')
        if 'account_id' in columns:clauses.append('h.account_id=:a')
        if 'account_number' in columns:clauses.append('h.account_number=:n')
        if not clauses:continue
        history=db.session.execute(text("SELECT DISTINCT to_jsonb(h)->>'property_id' AS property_id,to_jsonb(h)->>'sectional_unit_id' AS sectional_unit_id,to_jsonb(h)->>'meter_id' AS meter_id,to_jsonb(h)->>'account_number' AS account_number FROM "+table+' h WHERE '+' OR '.join(clauses)),params).mappings().all()
        for row in history:
            row=dict(row);evidence.append(dict(table=table,**row));historical_scope(row,'Historical record')
            if row['account_number'] not in (None,'',number):conflicts.append('Historical records identify a different municipal account for a related meter.')
            if row['meter_id'] and row['meter_id'] not in {str(i) for i in ids}:conflicts.append('Historical account records refer to a meter outside the verified configuration.')
    if catalog.has_table('bil_architecture_draft'):
        drafts=db.session.execute(text("SELECT d.id,d.property_id FROM bil_architecture_draft d WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(to_jsonb(d.draft_json)->'accounts')='array' THEN to_jsonb(d.draft_json)->'accounts' ELSE '[]'::jsonb END) entry WHERE lower(btrim(entry->>'number'))=lower(btrim(:n))) ORDER BY d.id"),dict(n=number)).mappings().all()
        for row in drafts:
            evidence.append(dict(table='bil_architecture_draft',id=row['id'],property_id=row['property_id']))
            if row['property_id']!=property_id:conflicts.append('A saved historical architecture draft identifies this account under another property.')
    evidence.sort(key=lambda row:json.dumps(row,sort_keys=True,default=str))
    fingerprint=hashlib.sha256(json.dumps([account,meters,evidence,sorted(set(conflicts))],sort_keys=True,default=str).encode()).hexdigest()
    return dict(account=account,meters=meters,conflicts=sorted(set(conflicts)),fingerprint=fingerprint)

def serializer():return URLSafeTimedSerializer(current_app.secret_key,salt='pm-municipal-association')

def preview(property_id,account_id):
    plan=state(property_id,account_id)
    return dict(can_associate=not plan['conflicts'],association_conflicts=plan['conflicts'],association_token=serializer().dumps(dict(p=property_id,a=account_id,u=current_user.id,v=plan['fingerprint'])) if not plan['conflicts'] else '')

def associate(property_id,form):
    property_scope(property_id,lock=True)
    if form.get('association_confirmed')!='yes':abort(400,description='Explicitly confirm that you verified this account belongs to the selected property.')
    try:reviewed=serializer().loads(form.get('association_token',''),max_age=1800)
    except (BadSignature,SignatureExpired):abort(409,description='Association review expired or is invalid. Look up the account again before confirming.')
    if reviewed.get('p')!=property_id or reviewed.get('u')!=current_user.id:abort(403)
    account_id=reviewed.get('a')
    if str(form.get('account_id'))!=str(account_id):abort(400,description='Account identity does not match the verified review.')
    plan=state(property_id,account_id,lock=True)
    identity='PM_MUNICIPAL:account:'+(plan['account'].get('account_number') or '').strip().casefold()
    key=int.from_bytes(hashlib.sha256(identity.encode()).digest()[:8],'big',signed=True)
    db.session.execute(text('SELECT pg_advisory_xact_lock(:key)'),dict(key=key))
    # Recheck after acquiring both row and identity locks.
    plan=state(property_id,account_id,lock=True)
    if plan['conflicts']:abort(409,description='Association blocked: '+' '.join(plan['conflicts']))
    if plan['fingerprint']!=reviewed.get('v'):abort(409,description='Account or historical relationships changed. Review the lookup again before association.')
    linked=db.session.execute(text('UPDATE bil_muni_account SET property_id=:p WHERE id=:a AND property_id IS NULL RETURNING id'),dict(p=property_id,a=account_id)).scalar()
    if linked is None:abort(409,description='Account association changed; no reassignment was performed.')
    for meter in plan['meters']:
        db.session.execute(text('UPDATE bil_meter SET property_id=:p WHERE id=:m AND property_id IS NULL'),dict(p=property_id,m=meter['id']))
    # Unique, verified historical meters may be identified by the saved number rather than a pointer.
    for pointer,types in [('water_meter_id',('water',)),('elec_meter_id',('electricity','electrical'))]:
        group=[m for m in plan['meters'] if (m.get('utility_type') or '').lower() in types]
        if not plan['account'].get(pointer) and len(group)==1:
            db.session.execute(text('UPDATE bil_muni_account SET '+pointer+'=:m WHERE id=:a AND '+pointer+' IS NULL'),dict(m=group[0]['id'],a=account_id))
    return account_id
