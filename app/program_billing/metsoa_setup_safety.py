"""Existing METSOA architecture is view-only until non-destructive editing is implemented."""
import hashlib
from flask import abort
from flask_login import current_user
from sqlalchemy import inspect,text
from app.extensions import db
# :p must first pass property_scope(); identifiers are scoped through the existing manager relation.
PORTFOLIO_IDS='SELECT owned.id FROM bil_property owned WHERE owned.manager_id=(SELECT manager_id FROM bil_property WHERE id=:p)'


def account_visibility(alias='a'):
    # Unassigned historical accounts require positive ownership evidence, never number knowledge.
    return "("+alias+".property_id IN ("+PORTFOLIO_IDS+") OR ("+alias+".property_id IS NULL AND EXISTS(SELECT 1 FROM bil_meter linked WHERE (linked.id="+alias+".water_meter_id OR linked.id="+alias+".elec_meter_id) AND linked.property_id IN ("+PORTFOLIO_IDS+")) AND NOT EXISTS(SELECT 1 FROM bil_meter linked WHERE (linked.id="+alias+".water_meter_id OR linked.id="+alias+".elec_meter_id) AND (linked.property_id IS NULL OR linked.property_id NOT IN ("+PORTFOLIO_IDS+")))))"


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
        OR EXISTS(SELECT 1 FROM bil_meter WHERE property_id=:p)"""),dict(p=property_id)).scalar()
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
        if accounts and db.session.execute(text('SELECT EXISTS(SELECT 1 FROM bil_muni_account WHERE property_id IN ('+PORTFOLIO_IDS+') AND account_number=ANY(:values))'),dict(values=list(accounts),p=property_id)).scalar():
            abort(409,description='A municipal account number already exists. Existing configurations cannot be replaced or reassigned through Setup.')
        if meters and db.session.execute(text('SELECT EXISTS(SELECT 1 FROM bil_meter WHERE property_id IN ('+PORTFOLIO_IDS+') AND meter_number=ANY(:values))'),dict(values=list(meters),p=property_id)).scalar():
            abort(409,description='A meter number already exists. Existing meters and their history cannot be replaced or reassigned through Setup.')
    return property_id


def owned_account_id(number,property_id=None):
    """Legacy number-based endpoints must resolve an ID inside the signed-in portfolio."""
    if property_id:property_id=property_scope(property_id)
    rows=db.session.execute(text("""SELECT a.id FROM bil_muni_account a
        JOIN bil_property p ON p.id=a.property_id
        WHERE p.manager_id=:u AND lower(btrim(a.account_number))=lower(btrim(:n))
        AND (:p IS NULL OR a.property_id=:p) ORDER BY a.id"""),dict(u=current_user.id,n=number or '',p=property_id)).scalars().all()
    if not rows:abort(404,description='Municipal account not found in your property portfolio.')
    if len(rows)!=1:abort(409,description='Select the property for this municipal account.')
    return rows[0]
