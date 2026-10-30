"""Independent property-scoped municipal accounts. No bulk or history mutations."""
import hashlib,json,re
from datetime import datetime,date
from decimal import Decimal,InvalidOperation
from zoneinfo import ZoneInfo
from flask import request,abort,render_template,redirect,url_for,flash,current_app
from sqlalchemy import text,inspect
from sqlalchemy.exc import IntegrityError
from werkzeug.exceptions import HTTPException
from app.extensions import db
from .metsoa_setup_safety import property_scope

BASELINE_FIELDS=('water_opening_reading','water_opening_date','electricity_opening_reading','electricity_opening_date')
FIELDS=('account_number','owner_name','water_number','electricity_number')+BASELINE_FIELDS

def opening(meter):
    if not meter:return None
    return db.session.execute(text('SELECT id,reading_date,reading_value FROM bil_meter_reading WHERE meter_id=:m ORDER BY reading_date,id LIMIT 1'),dict(m=meter['id'])).mappings().first()

def baseline(kind,form,meter):
    existing=opening(meter)
    reading=str(form.get(kind+'_opening_reading') or '').strip()
    day=str(form.get(kind+'_opening_date') or '').strip()
    if existing:
        try:same=(not reading or Decimal(reading)==Decimal(str(existing['reading_value']))) and (not day or day==existing['reading_date'].isoformat())
        except InvalidOperation:same=False
        if not same:abort(409,description='The existing '+kind+' opening reading is protected. Editing account details cannot replace historical readings.')
        return None
    if meter and db.session.execute(text('SELECT 1 FROM bil_consumption WHERE meter_id=:m LIMIT 1'),dict(m=meter['id'])).first():
        abort(409,description='This meter has consumption history but no saved reading baseline. Verify its historical opening reading before adding one.')
    try:
        value=Decimal(reading)
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',day):raise ValueError()
        parsed=date.fromisoformat(day)
        if not value.is_finite() or value<0 or value>Decimal('1000000000000') or parsed.year<1900 or parsed>datetime.now(ZoneInfo('Africa/Johannesburg')).date():raise ValueError()
    except (ValueError,InvalidOperation):abort(400,description='Enter a valid non-negative '+kind+' opening reading (maximum 1,000,000,000,000) and a reading date from 1900 through today.')
    return dict(value=value,day=parsed)

def account_rows(property_id):
    return [dict(r) for r in db.session.execute(text('SELECT a.*,o.name AS owner_name FROM bil_muni_account a LEFT JOIN ref_muni_owner o ON o.id=a.owner_id WHERE a.property_id=:p ORDER BY a.id'),dict(p=property_id)).mappings()]

def meters_for(account):
    result={};issues=[]
    for kind,pointer,field in [('water','water_meter_id','muni_water_meter_no'),('electricity','elec_meter_id','muni_elec_meter_no')]:
        if account[pointer]:
            rows=db.session.execute(text('SELECT * FROM bil_meter WHERE id=:id AND property_id=:p'),dict(id=account[pointer],p=account['property_id'])).mappings().all()
            if not rows:issues.append('An existing meter pointer has no verified link to this property; this account is view-only.')
        else:
            rows=db.session.execute(text("SELECT * FROM bil_meter WHERE property_id=:p AND municipal_bill_number=:n AND lower(utility_type) = ANY(:types) ORDER BY id"),dict(p=account['property_id'],n=account['account_number'],types=['water'] if kind=='water' else ['electricity','electrical'])).mappings().all()
        if len(rows)>1:issues.append('Multiple '+kind+' meters are linked to this account; use the original wizard to view the complete configuration. This page cannot edit it safely.')
        result[kind]=dict(rows[0]) if len(rows)==1 else None
        if result[kind] and (result[kind]['utility_type'] or '').lower() not in (['water'] if kind=='water' else ['electricity','electrical']):
            issues.append('An existing meter type conflicts with its account pointer; this account is view-only.')
        if result[kind] and result[kind]['municipal_bill_number'] not in (None,'',account['account_number']):
            issues.append('The existing meter pointer and municipal account number disagree; this account is view-only until verified.')
    return result,issues

def token(account,meters):
    return hashlib.sha256(json.dumps([account,meters],sort_keys=True,default=str).encode()).hexdigest()

def has_history(meter):
    return db.session.execute(text('SELECT EXISTS(SELECT 1 FROM bil_meter_reading WHERE meter_id=:m) OR EXISTS(SELECT 1 FROM bil_consumption WHERE meter_id=:m)'),dict(m=meter['id'])).scalar()

def clean(form):
    data={k:str(form.get(k) or '').strip() for k in FIELDS}
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9 ./_-]{0,49}',data['account_number']):abort(400,description='Enter a municipal account number of 1–50 letters, digits, spaces, dots, slashes or hyphens.')
    if not data['owner_name'] or len(data['owner_name'])>100 or any(ord(c)<32 for c in data['owner_name']):abort(400,description='Enter an account owner name of 1–100 characters.')
    for key in ('water_number','electricity_number'):
        if data[key] and not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9./_-]{0,49}',data[key]):abort(400,description='Meter identifiers must contain 1–50 letters, digits, dots, slashes or hyphens, without spaces.')
    if data['water_number'] and data['water_number'].casefold()==data['electricity_number'].casefold():abort(400,description='Water and electricity meters must have different identifiers.')
    return data

def save(property_id,form):
    property_id=property_scope(property_id,lock=True)
    data=clean(form)
    raw=form.get('account_id')
    try:account_id=int(raw) if raw else None
    except (TypeError,ValueError):abort(400,description='Invalid municipal account.')
    if account_id is not None and account_id<=0:abort(400,description='Invalid municipal account.')
    account=next((a for a in account_rows(property_id) if a['id']==account_id),None) if account_id else None
    if account_id and not account:abort(404)
    meters,issues=meters_for(account) if account else ({'water':None,'electricity':None},[])
    if issues:abort(409,description=' '.join(issues))
    if account and form.get('version')!=token(account,meters):abort(409,description='This account changed after the form was opened. Reopen Edit before saving; your entered values remain below.')
    if not account and not (data['water_number'] or data['electricity_number']):abort(400,description='Enter at least one water or electricity meter number for a new account.')
    identities=['account:'+data['account_number'].casefold()]+['meter:'+data[k].casefold() for k in ('water_number','electricity_number') if data[k]]
    for identity in sorted(identities):
        lock=int.from_bytes(hashlib.sha256(('PM_MUNICIPAL:'+identity).encode()).digest()[:8],'big',signed=True)
        db.session.execute(text('SELECT pg_advisory_xact_lock(:key)'),dict(key=lock))
    duplicate=db.session.execute(text('SELECT id FROM bil_muni_account WHERE property_id=:p AND lower(btrim(account_number))=lower(:n) AND (:id IS NULL OR id<>:id) LIMIT 1'),dict(p=property_id,n=data['account_number'],id=account_id)).first()
    if duplicate:abort(409,description='This municipal account number already exists for the selected property.')
    if not account and db.session.execute(text('SELECT 1 FROM bil_muni_account WHERE property_id IS NULL AND lower(btrim(account_number))=lower(:n) LIMIT 1'),dict(n=data['account_number'])).first():
        abort(409,description='An unassociated historical account already uses this number. Verify its property association before creating another account.')
    if account and data['account_number']!=account['account_number']:
        if any(has_history(m) for m in meters.values() if m):abort(409,description='This account has meter history. Its historical account number cannot be changed through this page.')
        catalog=inspect(db.session.connection())
        for table in ('bil_muni_cycle_totals','bil_metsoa_cycle','bil_muni_payment'):
            if catalog.has_table(table) and db.session.execute(text('SELECT 1 FROM '+table+' WHERE account_id=:id LIMIT 1'),dict(id=account_id)).first():abort(409,description='This account has historical billing records; its account number is protected.')
    new_meters=[]
    for kind,key in [('water','water_number'),('electricity','electricity_number')]:
        meter=meters[kind];number=data[key]
        if meter and not number:abort(409,description='Existing meters cannot be removed through this page. Their records and history have been retained.')
        if meter:
            if db.session.execute(text('SELECT 1 FROM bil_muni_account WHERE id<>:a AND (water_meter_id=:m OR elec_meter_id=:m) LIMIT 1'),dict(a=account_id,m=meter['id'])).first():abort(409,description='This meter is referenced by another municipal account; it cannot be edited here.')
            if number!=meter['meter_number'] and has_history(meter):abort(409,description='This meter has reading or consumption history. Its identifier is protected; no records were changed.')
        if number:
            if db.session.execute(text('SELECT 1 FROM bil_meter WHERE lower(btrim(meter_number))=lower(:n) AND (:id IS NULL OR id<>:id) LIMIT 1'),dict(n=number,id=meter['id'] if meter else None)).first():abort(409,description='A meter with this identifier already exists. It cannot be duplicated or reassigned through this page.')
            if not meter:new_meters.append(kind)
    if new_meters:
        for c in inspect(db.session.connection()).get_columns('bil_meter'):
            if c['name']=='sectional_unit_id' and not c['nullable']:
                abort(503,description='Cannot create this meter: legacy bil_meter.sectional_unit_id is mandatory. A separately approved schema correction is required; no account or meter changes were saved.')
    baselines={}
    for kind,key in [('water','water_number'),('electricity','electricity_number')]:
        if data[key]:baselines[kind]=baseline(kind,form,meters[kind])
        elif form.get(kind+'_opening_reading') or form.get(kind+'_opening_date'):
            abort(400,description='An opening reading requires its corresponding meter number.')
    # Owner names can be shared: never rename a shared owner record.
    db.session.execute(text('INSERT INTO ref_muni_owner(name) VALUES(:n) ON CONFLICT(name) DO NOTHING'),dict(n=data['owner_name']))
    owner_id=db.session.execute(text('SELECT id FROM ref_muni_owner WHERE name=:n'),dict(n=data['owner_name'])).scalar()
    for kind,key in [('water','water_number'),('electricity','electricity_number')]:
        if not data[key]:continue
        values=dict(p=property_id,n=data[key],a=data['account_number'])
        if meters[kind]:
            values['id']=meters[kind]['id']
            db.session.execute(text('UPDATE bil_meter SET meter_number=:n,municipal_bill_number=:a WHERE id=:id AND property_id=:p'),values)
        else:
            values['u']='Water' if kind=='water' else 'Electrical'
            identifier=db.session.execute(text("INSERT INTO bil_meter(property_id,meter_number,utility_type,municipal_bill_number,status) VALUES(:p,:n,:u,:a,'active') RETURNING id"),values).scalar()
            meters[kind]=dict(id=identifier)
    values=dict(p=property_id,n=data['account_number'],o=owner_id,w=meters['water']['id'] if meters['water'] else None,e=meters['electricity']['id'] if meters['electricity'] else None,wn=data['water_number'] or None,en=data['electricity_number'] or None)
    if account:
        values['id']=account_id
        db.session.execute(text('UPDATE bil_muni_account SET account_number=:n,owner_id=:o,water_meter_id=:w,elec_meter_id=:e,muni_water_meter_no=:wn,muni_elec_meter_no=:en WHERE id=:id AND property_id=:p'),values)
    else:
        account_id=db.session.execute(text('INSERT INTO bil_muni_account(property_id,account_number,owner_id,water_meter_id,elec_meter_id,muni_water_meter_no,muni_elec_meter_no,is_bulk_account) VALUES(:p,:n,:o,:w,:e,:wn,:en,false) RETURNING id'),values).scalar()
    for kind,entry in baselines.items():
        if entry is not None:
            # Property-row lock serialises this page and the existing reading service.
            # No existing reading is updated; no consumption or charge is generated.
            meter_id=meters[kind]['id']
            if db.session.execute(text('SELECT 1 FROM bil_meter_reading WHERE meter_id=:m LIMIT 1'),dict(m=meter_id)).first():
                abort(409,description='A reading was recorded while saving. Reopen the account before retrying.')
            db.session.execute(text('INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(:m,:d,:v)'),dict(m=meter_id,d=entry['day'],v=entry['value']))
    return account_id

def page(property_id):
    property_scope(property_id)
    prop=db.session.execute(text('SELECT id,name,address FROM bil_property WHERE id=:p'),dict(p=property_id)).mappings().one()
    month=request.values.get('billing_month') or datetime.now().strftime('%Y-%m')
    if not re.fullmatch(r'\d{4}-(0[1-9]|1[0-2])',month):abort(400)
    error=None;status=200;values={k:'' for k in FIELDS};version='';account_id=None;established={'water':False,'electricity':False}
    if request.method=='POST':
        values={k:request.form.get(k,'') for k in FIELDS};version=request.form.get('version','');account_id=request.form.get('account_id')
        try:
            action=request.form.get('save_action','finish')
            if action not in ('add_another','finish'):abort(400,description='Choose a valid save action.')
            save(property_id,request.form);db.session.commit()
            flash('Municipal account saved with opening readings. Existing history was preserved; no consumption charges were generated.','success')
            return redirect(url_for('billing_bp.municipal_accounts',property_id=property_id,billing_month=month,**({'view':'list'} if action=='finish' else {})))
        except HTTPException as exc:
            db.session.rollback();error=exc.description;status=exc.code
        except IntegrityError as exc:
            db.session.rollback();current_app.logger.exception('Municipal account database constraint blocked saving')
            diagnostic=getattr(exc.orig,'diag',None)
            fields=[getattr(diagnostic,k,None) for k in ('table_name','column_name','constraint_name')]
            detail=' / '.join(str(value) for value in fields if value) or 'unspecified database constraint'
            error='Database constraint blocked saving: '+detail+'. No changes were committed.';status=503
        except Exception:
            db.session.rollback();current_app.logger.exception('Independent municipal account save failed')
            error='The account could not be saved because of a database constraint or storage error. No changes were committed; check the server log for the exact constraint.';status=503
    accounts=account_rows(property_id);display=[]
    for account in accounts:
        meters,issues=meters_for(account)
        item=dict(account);item['issues']=issues
        for kind in ('water','electricity'):
            recorded=opening(meters[kind]);item[kind+'_opening_reading']=str(recorded['reading_value']) if recorded else ''
            item[kind+'_opening_date']=recorded['reading_date'].isoformat() if recorded else ''
            if str(account_id)==str(account['id']):established[kind]=bool(recorded)
        item['water_number']=meters['water']['meter_number'] if meters['water'] else account['muni_water_meter_no'] or ''
        item['electricity_number']=meters['electricity']['meter_number'] if meters['electricity'] else account['muni_elec_meter_no'] or ''
        display.append(item)
    if request.method=='GET' and request.args.get('edit'):
        try:account_id=int(request.args['edit'])
        except ValueError:abort(400)
        account=next((a for a in accounts if a['id']==account_id),None)
        if not account:abort(404)
        meters,issues=meters_for(account);item=next(a for a in display if a['id']==account_id)
        values={k:item.get(k) or '' for k in FIELDS};version=token(account,meters)
        established={kind:bool(opening(meters[kind])) for kind in ('water','electricity')}
        if issues:error=' '.join(issues)
    return render_template('program_billing/municipal_accounts.html',property=prop,accounts=display,values=values,version=version,account_id=account_id,error=error,utility_month=month,established=established,today=datetime.now(ZoneInfo('Africa/Johannesburg')).date().isoformat(),show_form=request.method=='POST' or bool(account_id) or request.args.get('view')!='list'),status
