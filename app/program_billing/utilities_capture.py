"""Property-scoped reading and statement capture. No financial posting."""
import json,hashlib,re
from datetime import date,datetime
from decimal import Decimal,InvalidOperation
from zoneinfo import ZoneInfo
from flask import abort,request,render_template,redirect,url_for,flash,current_app
from flask_login import current_user
from sqlalchemy import text,inspect
from werkzeug.exceptions import HTTPException
from app.extensions import db
from .metsoa_setup_safety import property_scope

def fingerprint(row):return hashlib.sha256(json.dumps(dict(row),sort_keys=True,default=str).encode()).hexdigest()
def day(value):
    try:
        if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):raise ValueError()
        parsed=date.fromisoformat(value)
        if parsed.year<1900:raise ValueError()
        return parsed
    except (ValueError,TypeError):abort(400,description='Enter a valid date from 1900 onward.')
def amount(value,currency=False):
    try:
        value=Decimal(str(value))
        if not value.is_finite() or abs(value)>Decimal('999999999999.99') or (not currency and value<0):raise ValueError()
        if currency and value!=value.quantize(Decimal('.01')):raise ValueError()
        return value
    except (InvalidOperation,ValueError,TypeError):abort(400,description='Enter a finite amount'+(' with at most two decimal places.' if currency else ' that is not negative.'))
def available(table,columns):
    catalog=inspect(db.session.connection())
    return catalog.has_table(table) and set(columns)<={c['name'] for c in catalog.get_columns(table)}
def schema_ready():
    return available('bil_municipal_statement',['id','account_id','period','statement_date','arrears','balance','due','due_date','version']) and available('bil_utilities_capture_audit',['id','property_id','kind','record_id','actor_id','before_data','after_data'])
def ready(label='Metro Reconciliation'):
    if not schema_ready():abort(503,description=label+' is temporarily unavailable. Please contact the administrator.')
def audit(p,kind,record,before,after):
    db.session.execute(text('INSERT INTO bil_utilities_capture_audit(property_id,kind,record_id,actor_id,before_data,after_data) VALUES(:p,:k,:r,:u,CAST(:b AS json),CAST(:a AS json))'),dict(p=p,k=kind,r=record,u=current_user.id,b=json.dumps(dict(before),default=str) if before else None,a=json.dumps(dict(after),default=str)))
def meters(p):
    return [dict(r) for r in db.session.execute(text('SELECT id,meter_number,utility_type FROM bil_meter WHERE property_id=:p ORDER BY meter_number,id'),dict(p=p)).mappings()]
def readings(p):
    return [dict(r) for r in db.session.execute(text('SELECT r.*,m.meter_number,m.utility_type FROM bil_meter_reading r JOIN bil_meter m ON m.id=r.meter_id WHERE m.property_id=:p ORDER BY r.reading_date DESC,r.id DESC'),dict(p=p)).mappings()]
def save_reading(p,form):
    property_scope(p,lock=True);ready('Meter Readings')
    try:meter_id=int(form.get('meter_id',''));record_id=int(form['record_id']) if form.get('record_id') else None
    except ValueError:abort(400,description='Select a valid meter or reading.')
    meter=db.session.execute(text('SELECT * FROM bil_meter WHERE id=:m AND property_id=:p FOR UPDATE'),dict(m=meter_id,p=p)).mappings().first()
    if not meter:abort(403)
    new_date=day(form.get('reading_date'));value=amount(form.get('reading_value'))
    if new_date>datetime.now(ZoneInfo('Africa/Johannesburg')).date():abort(400,description='Reading date cannot be in the future.')
    original=None
    if record_id:
        original=db.session.execute(text('SELECT * FROM bil_meter_reading WHERE id=:r AND meter_id=:m FOR UPDATE'),dict(r=record_id,m=meter_id)).mappings().first()
        if not original:abort(404)
        if form.get('version')!=fingerprint(original):abort(409,description='Reading changed. Reopen Edit; entered values are preserved below.')
        if original['reading_date']==new_date and Decimal(str(original['reading_value']))==value:return record_id
        # Consumed or superseded readings cannot be changed by an ordinary capture form.
        if db.session.execute(text('SELECT 1 FROM bil_consumption WHERE meter_id=:m LIMIT 1'),dict(m=meter_id)).first() or db.session.execute(text('SELECT 1 FROM bil_meter_reading WHERE meter_id=:m AND reading_date>:d AND id<>:r LIMIT 1'),dict(m=meter_id,d=original['reading_date'],r=record_id)).first():
            abort(409,description='This reading supports historical consumption or later readings. A separately reviewed correction is required; history is unchanged.')
    duplicate=db.session.execute(text('SELECT * FROM bil_meter_reading WHERE meter_id=:m AND reading_date=:d AND (:r IS NULL OR id<>:r)'),dict(m=meter_id,d=new_date,r=record_id)).mappings().all()
    if duplicate:
        if not record_id and len(duplicate)==1 and Decimal(str(duplicate[0]['reading_value']))==value:return duplicate[0]['id']
        abort(409,description='A reading already exists for this meter and date. Use Edit rather than creating a duplicate.')
    previous=db.session.execute(text('SELECT reading_value FROM bil_meter_reading WHERE meter_id=:m AND reading_date<:d AND (:r IS NULL OR id<>:r) ORDER BY reading_date DESC,id DESC LIMIT 1'),dict(m=meter_id,d=new_date,r=record_id)).scalar()
    following=db.session.execute(text('SELECT reading_value FROM bil_meter_reading WHERE meter_id=:m AND reading_date>:d AND (:r IS NULL OR id<>:r) ORDER BY reading_date,id LIMIT 1'),dict(m=meter_id,d=new_date,r=record_id)).scalar()
    if previous is not None and value<Decimal(str(previous)):abort(400,description='Reading cannot be below the immediately preceding saved reading ('+str(previous)+'). Existing readings are unchanged.')
    if following is not None and value>Decimal(str(following)):abort(400,description='Reading cannot exceed the immediately following saved reading ('+str(following)+'). Existing readings are unchanged.')
    data=dict(m=meter_id,d=new_date,v=value,r=record_id)
    if original:db.session.execute(text('UPDATE bil_meter_reading SET reading_date=:d,reading_value=:v WHERE id=:r'),data)
    elif previous is not None and following is None:
        from .utilities_readings import save as save_with_consumption
        save_with_consumption(dict(meter_id=meter_id,property_id=p,date=new_date.isoformat(),reading_month=new_date.strftime('%Y-%m'),reading=str(value)))
        record_id=db.session.execute(text('SELECT id FROM bil_meter_reading WHERE meter_id=:m AND reading_date=:d'),data).scalar()
    else:record_id=db.session.execute(text('INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(:m,:d,:v) RETURNING id'),data).scalar()
    audit(p,'reading',record_id,original,dict(meter_id=meter_id,reading_date=new_date,reading_value=str(value)))
    return record_id

def paid(account,period):
    if not available('bil_muni_payment',['account_id','month','amount']):return None
    # Only explicit municipal account/period allocations qualify. Never tenant/property totals.
    return db.session.execute(text('SELECT COALESCE(SUM(CAST(amount AS numeric(18,2))),0) FROM bil_muni_payment WHERE account_id=:a AND month=:p'),dict(a=account,p=period)).scalar()
def statements(p):
    if not available('bil_municipal_statement',['id','account_id','period','statement_date','arrears','balance','due','due_date','version']):return []
    rows=[dict(r) for r in db.session.execute(text('SELECT s.*,a.account_number FROM bil_municipal_statement s JOIN bil_muni_account a ON a.id=s.account_id WHERE a.property_id=:p ORDER BY s.statement_date DESC,s.id DESC'),dict(p=p)).mappings()]
    for row in rows:row['paid']=paid(row['account_id'],row['period'])
    return rows

def save_statement(p,form):
    property_scope(p,lock=True);ready()
    try:account_id=int(form.get('account_id',''));record_id=int(form['record_id']) if form.get('record_id') else None
    except ValueError:abort(400,description='Select a valid account or statement.')
    account=db.session.execute(text('SELECT id FROM bil_muni_account WHERE id=:a AND property_id=:p FOR UPDATE'),dict(a=account_id,p=p)).first()
    if not account:abort(403)
    statement_date=day(form.get('statement_date'));due_date=day(form.get('due_date'))
    if due_date<statement_date:abort(400,description='Due date cannot precede the statement date.')
    values=dict(a=account_id,d=statement_date,period=statement_date.strftime('%Y-%m'),due_date=due_date,r=record_id,**{k:amount(form.get(k),True) for k in ('arrears','balance','due')})
    old=None
    if record_id:
        old=db.session.execute(text('SELECT * FROM bil_municipal_statement WHERE id=:r AND account_id=:a FOR UPDATE'),values).mappings().first()
        if not old:abort(404)
        if str(old['version'])!=form.get('version'):abort(409,description='Statement changed. Reopen Edit; entered values are preserved below.')
        if old['period']!=values['period']:abort(409,description='Keep this statement in its original billing month. Capture a separate record for another month.')
    duplicate=db.session.execute(text('SELECT * FROM bil_municipal_statement WHERE account_id=:a AND period=:period AND (:r IS NULL OR id<>:r)'),values).mappings().first()
    if duplicate:
        if not record_id and all(duplicate[k]==values[k] for k in ('arrears','balance','due','due_date')) and duplicate['statement_date']==statement_date:return duplicate['id']
        abort(409,description='A statement already exists for this account and billing month. Use Edit.')
    if old:db.session.execute(text('UPDATE bil_municipal_statement SET statement_date=:d,period=:period,arrears=:arrears,balance=:balance,due=:due,due_date=:due_date,version=version+1 WHERE id=:r'),values)
    else:record_id=db.session.execute(text('INSERT INTO bil_municipal_statement(account_id,period,statement_date,arrears,balance,due,due_date) VALUES(:a,:period,:d,:arrears,:balance,:due,:due_date) RETURNING id'),values).scalar()
    audit(p,'statement',record_id,old,values)
    return record_id

def page(p,kind):
    property_scope(p)
    from .municipal_metsoa import month_context
    utility_month,_,_=month_context(request.values.get('billing_month'))
    prop=db.session.execute(text('SELECT id,name,address FROM bil_property WHERE id=:p'),dict(p=p)).mappings().one()
    error=None;status=200;values={};edit_id=request.args.get('edit');version=''
    if request.method=='POST':
        values=request.form.to_dict();edit_id=values.get('record_id')
        try:
            (save_reading if kind=='readings' else save_statement)(p,values);db.session.commit()
            flash('Meter reading saved.' if kind=='readings' else 'Municipal statement saved.','success')
            return redirect(url_for('billing_bp.meter_readings' if kind=='readings' else 'billing_bp.metro_reconciliation',property_id=p,billing_month=utility_month))
        except HTTPException as exc:db.session.rollback();error=exc.description;status=exc.code
        except Exception:
            db.session.rollback();current_app.logger.exception('Utilities capture failed');error='Saving failed; no changes were committed. Check the server log.';status=503
    rows=readings(p) if kind=='readings' else statements(p)
    if request.method=='GET' and edit_id:
        try:row=next((r for r in rows if r['id']==int(edit_id)),None)
        except ValueError:abort(400)
        if not row:abort(404)
        values=dict(row)
        if kind=='readings':
            canonical={k:row[k] for k in ('id','meter_id','reading_date','reading_value')};version=fingerprint(canonical)
        else:version=str(row['version'])
    elif request.method=='POST':version=values.get('version','')
    if not schema_ready():error=error or ('Metro Reconciliation is temporarily unavailable. Please contact the administrator.' if kind=='statements' else 'Meter Readings is temporarily unavailable. Please contact the administrator.')
    accounts=[dict(r) for r in db.session.execute(text('SELECT id,account_number FROM bil_muni_account WHERE property_id=:p ORDER BY account_number,id'),dict(p=p)).mappings()]
    return render_template('program_billing/utilities_capture.html',property=prop,kind=kind,utility_month=utility_month,rows=rows,meters=meters(p),accounts=accounts,values=values,version=version,edit_id=edit_id,error=error,capture_ready=schema_ready(),today=datetime.now(ZoneInfo('Africa/Johannesburg')).date().isoformat()),status
