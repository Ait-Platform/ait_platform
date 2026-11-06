"""Property-scoped readings, reconciliation and municipal payment capture."""
import json,hashlib,re,uuid
from datetime import date,datetime
from decimal import Decimal,InvalidOperation
from zoneinfo import ZoneInfo
from flask import abort,request,render_template,redirect,url_for,flash,current_app,get_flashed_messages
from flask_login import current_user
from sqlalchemy import text,inspect
from werkzeug.exceptions import HTTPException
from itsdangerous import URLSafeTimedSerializer,BadData
from app.extensions import db
from .metsoa_setup_safety import property_scope

PAYMENT_REFERENCE_SEQUENCE='bil_mar_payment_reference_seq'

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
class ReadingConfirmationRequired(Exception):
    def __init__(self,rows,token,mode='',target=None):self.rows=rows;self.token=token;self.mode=mode;self.target=target

class ReadingCaptureCancelled(Exception):pass


def confirm_additional_reading(p,meter_id,new_date,value,form):
    start=new_date.replace(day=1)
    end=date(start.year+start.month//12,start.month%12+1,1)
    rows=[dict(r) for r in db.session.execute(text("""SELECT r.id,r.reading_date,r.reading_value
        FROM bil_meter_reading r JOIN bil_meter m ON m.id=r.meter_id
        WHERE m.property_id=:p AND r.meter_id=:m AND r.reading_date>=:start AND r.reading_date<:end
        ORDER BY r.reading_date,r.id"""),dict(p=p,m=meter_id,start=start,end=end)).mappings()]
    requested=form.get('reading_duplicate_action','')
    if not rows:
        if requested in ('replace','confirm','review_replace','review_additional'):abort(409,description='The selected reading or month changed. Review the entered reading before saving.')
        return '',None
    mode='replace' if requested in ('review_replace','replace') else ('additional' if requested in ('review_additional','confirm') else '')
    try:target_id=int(form.get('reading_replace_id') or rows[-1]['id'])
    except (ValueError,TypeError):abort(400,description='Select a displayed reading to replace.')
    target=next((r for r in rows if r['id']==target_id),None)
    if not target:mode='';target=rows[-1];target_id=target['id']
    payload=dict(property_id=p,meter_id=meter_id,reading_date=new_date.isoformat(),reading_value=str(value),actor_id=current_user.id,existing=fingerprint(dict(rows=rows)),mode=mode,target_id=target_id if mode=='replace' else None)
    serializer=URLSafeTimedSerializer(current_app.secret_key,salt='pm-additional-meter-reading')
    if requested in ('replace','confirm'):
        try:
            if mode and serializer.loads(form.get('reading_duplicate_token',''),max_age=900)==payload:return mode,target
        except BadData:pass
    raise ReadingConfirmationRequired(rows,serializer.dumps(payload),mode,target_id)


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
    mode,target=confirm_additional_reading(p,meter_id,new_date,value,form) if not record_id else ('',None)
    if mode=='replace':
        original=db.session.execute(text('SELECT * FROM bil_meter_reading WHERE id=:r AND meter_id=:m FOR UPDATE'),dict(r=target['id'],m=meter_id)).mappings().one()
        record_id=original['id']
    additional=mode=='additional'
    before_rows=[dict(r) for r in db.session.execute(text('SELECT id,reading_date,reading_value FROM bil_meter_reading WHERE meter_id=:m ORDER BY reading_date,id'),dict(m=meter_id)).mappings()]
    proposed=[r for r in before_rows if r['id']!=record_id]+[dict(id=record_id or -1,reading_date=new_date,reading_value=value)]
    from .municipal_metsoa import month_context,reading_pair_from_rows
    from .reading_consumption import refresh
    periods={new_date.strftime('%Y-%m')}
    if original:periods.add(original['reading_date'].strftime('%Y-%m'))
    for period in list(periods):
        _,start,end=month_context(period)
        periods.add(date(start.year+start.month//12,start.month%12+1,1).strftime('%Y-%m'))
    for period in periods:
        _,start,end=month_context(period);pair=reading_pair_from_rows(meter,start,end,proposed)
        if pair['error'] and pair['error'].startswith('Readings decrease'):abort(400,description=pair['error'])
    data=dict(m=meter_id,d=new_date,v=value,r=record_id)
    if original:db.session.execute(text('UPDATE bil_meter_reading SET reading_date=:d,reading_value=:v WHERE id=:r'),data)
    else:record_id=db.session.execute(text('INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(:m,:d,:v) RETURNING id'),data).scalar()
    after_rows=[dict(r) for r in db.session.execute(text('SELECT id,reading_date,reading_value FROM bil_meter_reading WHERE meter_id=:m ORDER BY reading_date,id'),dict(m=meter_id)).mappings()]
    for consumption_id,before,after in refresh(db.session,meter,before_rows,after_rows,periods):audit(p,'reading_consumption',consumption_id,before,after)
    recorded=dict(meter_id=meter_id,reading_date=new_date,reading_value=str(value))
    if additional:recorded['additional_reading_confirmed']=True
    if mode=='replace':recorded['replacement_confirmed']=True
    audit(p,'reading',record_id,original,recorded)
    return record_id

class ReadingDeleteConfirmation(Exception):
    def __init__(self,reading,token):self.reading=reading;self.token=token


def delete_reading(p,form):
    property_scope(p,lock=True);ready('Meter Readings')
    try:meter_id=int(form.get('meter_id',''));record_id=int(form.get('record_id',''))
    except (ValueError,TypeError):abort(400,description='Reopen Edit before deleting a reading.')
    meter=db.session.execute(text('SELECT * FROM bil_meter WHERE id=:m AND property_id=:p FOR UPDATE'),dict(m=meter_id,p=p)).mappings().first()
    if not meter:abort(403)
    original=db.session.execute(text('SELECT * FROM bil_meter_reading WHERE id=:r AND meter_id=:m FOR UPDATE'),dict(r=record_id,m=meter_id)).mappings().first()
    if not original:abort(404,description='This reading is no longer available.')
    if form.get('version')!=fingerprint(original):abort(409,description='Reading changed. Reopen Edit before deleting it.')
    serializer=URLSafeTimedSerializer(current_app.secret_key,salt='pm-delete-meter-reading')
    payload=dict(property_id=p,meter_id=meter_id,record_id=record_id,actor_id=current_user.id,version=fingerprint(original))
    confirmed=False
    if form.get('action')=='confirm_delete_reading':
        try:confirmed=serializer.loads(form.get('reading_delete_token',''),max_age=900)==payload
        except BadData:pass
    if not confirmed:raise ReadingDeleteConfirmation(dict(original,meter_number=meter['meter_number']),serializer.dumps(payload))
    before=[dict(r) for r in db.session.execute(text('SELECT id,reading_date,reading_value FROM bil_meter_reading WHERE meter_id=:m ORDER BY reading_date,id'),dict(m=meter_id)).mappings()]
    db.session.execute(text('DELETE FROM bil_meter_reading WHERE id=:r AND meter_id=:m'),dict(r=record_id,m=meter_id))
    after=[r for r in before if r['id']!=record_id]
    start=original['reading_date'].replace(day=1)
    periods={start.strftime('%Y-%m'),date(start.year+start.month//12,start.month%12+1,1).strftime('%Y-%m')}
    from .reading_consumption import refresh
    for consumption_id,old,new in refresh(db.session,meter,before,after,periods):audit(p,'reading_consumption',consumption_id,old,new)
    audit(p,'reading',record_id,original,dict(deleted=True,meter_id=meter_id,meter_number=meter['meter_number'],reading_date=original['reading_date'],reading_value=str(original['reading_value'])))


def paid(account,period):
    if not available('bil_muni_payment',['account_id','month','amount']):return None
    # Only explicit municipal account/period allocations qualify. Never tenant/property totals.
    return db.session.execute(text('SELECT COALESCE(SUM(CAST(amount AS numeric(18,2))),0) FROM bil_muni_payment WHERE account_id=:a AND month=:p'),dict(a=account,p=period)).scalar()
def payment_ready():
    return schema_ready() and available('bil_muni_payment',['id','account_id','month','payment_date','amount','reference']) and db.session.execute(text('SELECT to_regclass(:seq)'),dict(seq=PAYMENT_REFERENCE_SEQUENCE)).scalar() is not None


def record_payment(p,form):
    property_scope(p)
    if not payment_ready():abort(503,description='Municipal payment capture is temporarily unavailable. No changes were saved.')
    try:
        statement_id=int(form.get('statement_id',''))
        key=str(uuid.UUID(form.get('request_key','')))
        if statement_id<=0:raise ValueError()
    except (TypeError,ValueError,AttributeError):abort(400,description='Reopen Record Payment before saving.')
    payment_date=day(form.get('payment_date'))
    if payment_date>datetime.now(ZoneInfo('Africa/Johannesburg')).date():abort(400,description='Payment date cannot be in the future.')
    raw=str(form.get('payment_amount') or '')
    if not re.fullmatch(r'[0-9]+(?:\.[0-9]{1,2})?',raw):abort(400,description='Amount Paid must be a positive rand amount with at most two decimal places.')
    value=amount(raw,True)
    if value<=0:abort(400,description='Amount Paid must be greater than zero.')
    if db.session.execute(text('SHOW transaction_isolation')).scalar()!='read committed':
        abort(503,description='Municipal payment capture requires READ COMMITTED isolation.')
    db.session.execute(text("SET LOCAL lock_timeout = '5s'"))
    property_scope(p,lock=True)
    # The target record is the authority for both account and allocation period.
    statement=db.session.execute(text('''SELECT s.*,a.property_id,a.account_number FROM bil_municipal_statement s
        JOIN bil_muni_account a ON a.id=s.account_id
        WHERE s.id=:id AND a.property_id=:p FOR UPDATE OF s,a'''),dict(id=statement_id,p=p)).mappings().first()
    if not statement:abort(404)
    if form.get('account_id') and str(statement['account_id'])!=str(form['account_id']):
        abort(403,description='This payment must use the reconciliation record municipal account.')
    canonical=dict(statement_id=statement_id,account_id=statement['account_id'],period=statement['period'],
                   payment_date=payment_date.isoformat(),amount=str(value.quantize(Decimal('.01'))))
    digest=hashlib.sha256(json.dumps(canonical,sort_keys=True).encode()).hexdigest()
    db.session.execute(text('SELECT pg_advisory_xact_lock(hashtextextended(:key,0))'),dict(key='pm-reconciliation-payment:'+key))
    receipt=db.session.execute(text('''SELECT * FROM bil_utilities_capture_audit
        WHERE kind='municipal_payment' AND after_data->>'request_key'=:key ORDER BY id LIMIT 1'''),dict(key=key)).mappings().first()
    if receipt:
        # Retain replay compatibility with historical receipts which hashed an entered/account reference.
        legacy_digest=hashlib.sha256(json.dumps(dict(canonical,reference=receipt['after_data'].get('reference')),sort_keys=True).encode()).hexdigest()
        if receipt['property_id']!=p or receipt['actor_id']!=current_user.id or receipt['after_data'].get('payload_hash') not in (digest,legacy_digest):
            abort(409,description='This payment submission was already used with different information. Reopen Record Payment.')
        return receipt['record_id'],statement['account_id']
    duplicate=db.session.execute(text('''SELECT id FROM bil_muni_payment
        WHERE account_id=:a AND payment_date=:d AND CAST(amount AS numeric(18,2))=:v
        LIMIT 1'''),
        dict(a=statement['account_id'],d=payment_date,v=value)).first()
    if duplicate:abort(409,description='This municipal payment is already recorded. Existing transactions were retained.')
    # PostgreSQL nextval is atomic across sessions and never reuses a rolled-back allocation.
    # Skip any historical reference which already happens to use this padded numeric form.
    while True:
        number=db.session.execute(text('SELECT nextval(:seq)'),dict(seq=PAYMENT_REFERENCE_SEQUENCE)).scalar_one()
        reference=format(number,'03d')
        if not db.session.execute(text('SELECT 1 FROM bil_muni_payment WHERE reference=:ref LIMIT 1'),dict(ref=reference)).first():break
    before_paid=paid(statement['account_id'],statement['period'])
    payment_id=db.session.execute(text('''INSERT INTO bil_muni_payment(account_id,month,payment_date,amount,reference)
        VALUES(:a,:period,:d,:v,:ref) RETURNING id'''),
        dict(a=statement['account_id'],period=statement['period'],d=payment_date,v=value,ref=reference)).scalar_one()
    audit(p,'municipal_payment',payment_id,dict(paid=str(before_paid)),
          dict(canonical,reference=reference,payment_id=payment_id,request_key=key,payload_hash=digest,paid=str(before_paid+value)))
    return payment_id,statement['account_id']


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
    utility_month,period_start,period_end=month_context(request.values.get('billing_month'))
    prop=db.session.execute(text('SELECT id,name,address FROM bil_property WHERE id=:p'),dict(p=p)).mappings().one()
    error=None;status=200;values={};edit_id=request.args.get('edit');version=''
    reading_duplicates=[];reading_duplicate_token='';reading_duplicate_mode='';reading_replace_id=None;reading_delete=None;reading_delete_token=''
    payment_statement_id=request.args.get('payment') if kind=='statements' else None
    if request.method=='POST':
        values=request.form.to_dict();edit_id=values.get('record_id')
        if kind=='statements' and values.get('action')=='record_payment':payment_statement_id=values.get('statement_id')
        try:
            if kind=='readings' and values.get('action')=='cancel_delete_reading':raise ReadingCaptureCancelled()
            if kind=='readings' and values.get('action') in ('delete_reading','confirm_delete_reading'):
                delete_reading(p,values);db.session.commit()
                flash('Meter reading deleted.','success')
                return redirect(url_for('billing_bp.meter_readings',property_id=p,billing_month=utility_month,view_item=values.get('view_item',''),workspace='register'))
            if kind=='readings' and not edit_id and values.get('reading_duplicate_action')=='cancel':raise ReadingCaptureCancelled()
            if kind=='statements' and values.get('action')=='record_payment':
                _,account_id=record_payment(p,values);db.session.commit()
                flash('Municipal payment recorded. Existing payments and reconciliation amounts were preserved.','success')
                return redirect(url_for('billing_bp.metro_reconciliation',property_id=p,billing_month=utility_month,view_item=account_id))
            (save_reading if kind=='readings' else save_statement)(p,values);db.session.commit()
            flash('Meter reading saved.' if kind=='readings' else 'Municipal statement saved.','success')
            if kind=='readings' and not values.get('record_id'):
                return redirect(url_for('billing_bp.meter_readings',property_id=p,billing_month=utility_month,view_item=values.get('view_item',''),workspace='capture',reading_date=day(values.get('reading_date')).isoformat()))
            return redirect(url_for('billing_bp.meter_readings' if kind=='readings' else 'billing_bp.metro_reconciliation',property_id=p,billing_month=utility_month,view_item=values.get('view_item','')))
        except ReadingCaptureCancelled:db.session.rollback()
        except ReadingDeleteConfirmation as exc:
            db.session.rollback();reading_delete=exc.reading;reading_delete_token=exc.token
        except ReadingConfirmationRequired as exc:
            db.session.rollback();reading_duplicates=exc.rows;reading_duplicate_token=exc.token;reading_duplicate_mode=exc.mode;reading_replace_id=exc.target
        except HTTPException as exc:db.session.rollback();error=exc.description;status=exc.code
        except Exception:
            db.session.rollback();current_app.logger.exception('Utilities capture failed');error='Saving failed; no changes were committed. Check the server log.';status=503
    rows=readings(p) if kind=='readings' else statements(p)
    payment_target=None
    if kind=='statements' and payment_statement_id:
        try:payment_id=int(payment_statement_id)
        except (TypeError,ValueError):abort(400,description='Select a valid reconciliation record.')
        payment_target=next((r for r in rows if r['id']==payment_id),None)
        if not payment_target:abort(404)
    payment_history=[]
    if payment_target and available('bil_muni_payment',['id','account_id','month','payment_date','amount','reference']):
        payment_history=[dict(r) for r in db.session.execute(text('''SELECT id,payment_date,amount,reference
            FROM bil_muni_payment WHERE account_id=:account AND month=:period ORDER BY payment_date,id'''),
            dict(account=payment_target['account_id'],period=payment_target['period'])).mappings()]
    if request.method=='GET' and edit_id:
        try:row=next((r for r in rows if r['id']==int(edit_id)),None)
        except ValueError:abort(400)
        if not row:abort(404)
        values=dict(row)
        if kind=='readings':
            canonical={k:row[k] for k in ('id','meter_id','reading_date','reading_value')};version=fingerprint(canonical)
        else:version=str(row['version'])
    elif request.method=='POST':version=values.get('version','')
    if kind=='readings' and request.method=='GET' and not edit_id and request.args.get('workspace')=='capture' and request.args.get('reading_date'):
        values['reading_date']=day(request.args['reading_date']).isoformat()
    if not schema_ready():error=error or ('Metro Reconciliation is temporarily unavailable. Please contact the administrator.' if kind=='statements' else 'Meter Readings is temporarily unavailable. Please contact the administrator.')
    accounts=[dict(r) for r in db.session.execute(text('SELECT id,account_number FROM bil_muni_account WHERE property_id=:p ORDER BY account_number,id'),dict(p=p)).mappings()]
    reading_period_summary=None
    if kind=='readings':
        counts=db.session.execute(text('''SELECT lower(m.utility_type) AS utility,count(*) AS total
            FROM bil_meter_reading r JOIN bil_meter m ON m.id=r.meter_id
            JOIN bil_property p ON p.id=m.property_id
            WHERE p.manager_id=:user AND r.reading_date BETWEEN :start AND :end
            GROUP BY lower(m.utility_type)'''),dict(user=current_user.id,start=period_start,end=period_end)).mappings()
        portfolio={r['utility']:r['total'] for r in counts}
        reading_period_summary=dict(property=sum(period_start<=r['reading_date']<=period_end for r in rows),
            electricity=portfolio.get('electricity',0)+portfolio.get('electrical',0),water=portfolio.get('water',0))
    presentation={}
    if kind=='readings':
        messages=get_flashed_messages(with_categories=True)
        saved=not edit_id and request.args.get('workspace')=='capture' and ('success','Meter reading saved.') in messages
        remaining=[item for item in messages if not saved or item!=('success','Meter reading saved.')]
        def page_flashes(with_categories=False,category_filter=()):
            selected=[item for item in remaining if not category_filter or item[0] in category_filter]
            return selected if with_categories else [message for _,message in selected]
        presentation=dict(reading_saved=saved,get_flashed_messages=page_flashes)
    return render_template('program_billing/utilities_capture.html',property=prop,kind=kind,utility_month=utility_month,rows=rows,meters=meters(p),accounts=accounts,values=values,version=version,edit_id=edit_id,error=error,capture_ready=schema_ready(),reading_delete=reading_delete,reading_delete_token=reading_delete_token,reading_duplicates=reading_duplicates,reading_duplicate_token=reading_duplicate_token,reading_duplicate_mode=reading_duplicate_mode,reading_replace_id=reading_replace_id,reading_period_summary=reading_period_summary,payment_target=payment_target,payment_history=payment_history,payment_capture_ready=payment_ready() if kind=='statements' else False,payment_request_key=values.get('request_key') or str(uuid.uuid4()),today=datetime.now(ZoneInfo('Africa/Johannesburg')).date().isoformat(),**presentation),status
