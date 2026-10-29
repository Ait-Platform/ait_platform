"""Existing METSOA architecture is view-only until non-destructive editing is implemented."""
import hashlib
from flask import abort
from flask_login import current_user
from sqlalchemy import inspect,text
from app.extensions import db
MESSAGE='This property already has a METSOA configuration. Setup is view-only: saving is blocked to preserve municipal accounts, meters, readings and consumption history.'


def property_scope(property_id,lock=False):
    try:
        if isinstance(property_id,bool) or int(property_id)<=0:raise ValueError()
        property_id=int(property_id)
    except (TypeError,ValueError):abort(400,description='Select a valid property.')
    row=db.session.execute(text('SELECT id,manager_id FROM bil_property WHERE id=:id'+(' FOR UPDATE' if lock else '')),dict(id=property_id)).mappings().first()
    if row is None:abort(404)
    if row['manager_id']!=current_user.id:abort(403)
    return property_id


def reason(property_id):
    # Do not infer safety from missing legacy schema: fail closed for writes.
    catalog=inspect(db.session.connection())
    for table,required in [('bil_muni_account',{'id','property_id','account_number'}),('bil_meter',{'id','property_id','municipal_bill_number','meter_number'})]:
        if not catalog.has_table(table) or not required<={c['name'] for c in catalog.get_columns(table)}:
            return 'METSOA configuration safety could not be verified. Setup is view-only; saving is blocked.'
    existing=db.session.execute(text("""SELECT EXISTS(SELECT 1 FROM bil_muni_account WHERE property_id=:p)
        OR EXISTS(SELECT 1 FROM bil_meter WHERE property_id=:p OR municipal_bill_number IN
            (SELECT account_number FROM bil_muni_account WHERE property_id=:p))"""),dict(p=property_id)).scalar()
    return MESSAGE if existing else None


def protect_save(property_id,payload=None):
    property_id=property_scope(property_id,lock=True)
    blocked=reason(property_id)
    if blocked:abort(409,description=blocked)
    # New setup may not claim pre-existing (including unlinked) account/meter identities.
    if payload is not None and not isinstance(payload,dict):
        abort(400,description='Invalid setup payload.')
    if isinstance(payload,dict):
        for field in ('accounts','bulkWater','bulkElec','subWater','subElec','meters','readings'):
            if field in payload and not isinstance(payload[field],list):
                abort(400,description='Invalid setup list: '+field+'.')
        accounts={str(r.get('number') or '').strip() for r in payload.get('accounts',[]) if isinstance(r,dict)}
        accounts|={str(payload.get(k) or '').strip() for k in ('metro_account_no','municipal_bill_number')}
        meters=set()
        for key in ('bulkWater','bulkElec','subWater','subElec','meters','readings'):
            for item in payload.get(key,[]) or []:
                if isinstance(item,dict):
                    meters.add(str(item.get('number') or item.get('meter_number') or '').strip())
                    accounts.add(str(item.get('municipal_bill_number') or '').strip())
        accounts.discard('');meters.discard('')
        for scope in sorted(['ACCOUNT:'+s for s in accounts]+['METER:'+s for s in meters]):
            key=int.from_bytes(hashlib.sha256(('METSOA_SETUP:'+scope).encode()).digest()[:8],'big',signed=True)
            db.session.execute(text('SELECT pg_advisory_xact_lock(:key)'),dict(key=key))
        if accounts and db.session.execute(text('SELECT EXISTS(SELECT 1 FROM bil_muni_account WHERE account_number=ANY(:values))'),dict(values=list(accounts))).scalar():
            abort(409,description='A municipal account number already exists. Existing configurations cannot be replaced or reassigned through Setup.')
        if meters and db.session.execute(text('SELECT EXISTS(SELECT 1 FROM bil_meter WHERE meter_number=ANY(:values))'),dict(values=list(meters))).scalar():
            abort(409,description='A meter number already exists. Existing meters and their history cannot be replaced or reassigned through Setup.')
    return property_id
