"""Municipal arrangement monitoring only; never posts or reprices financial records."""
import calendar,hashlib,json,re,uuid
from datetime import date,datetime,timedelta
from decimal import Decimal,InvalidOperation
from zoneinfo import ZoneInfo
from flask import abort,request,flash,redirect,url_for,current_app
from flask_login import current_user
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError,SQLAlchemyError
from werkzeug.exceptions import HTTPException
from app.extensions import db
from .metsoa_setup_safety import property_scope

FREQUENCIES={'weekly':'Weekly','fortnightly':'Fortnightly','monthly':'Monthly','quarterly':'Quarterly'}
STATUSES={'active':'Active','on_hold':'On hold','cancelled':'Cancelled'}
TABLES=('bil_municipal_arrangement','bil_municipal_arrangement_payment','bil_municipal_arrangement_event')


def today():return datetime.now(ZoneInfo('Africa/Johannesburg')).date()


def can_manage():
    """Same active-user + ApprovedAdmin grant policy used by PM Core administration."""
    if not current_user.is_authenticated:return False
    return bool(db.session.execute(text("""SELECT EXISTS(SELECT 1 FROM "user" u
        JOIN auth_approved_admin a ON lower(a.email)=lower(u.email)
        WHERE u.id=:u AND CAST(u.is_active AS text) IN ('true','1')
          AND CAST(a.active AS text) IN ('true','1'))"""),dict(u=current_user.id)).scalar())


def scope(p):
    if not can_manage():abort(403)
    return property_scope(p)


def ready():
    return all(db.session.execute(text('SELECT to_regclass(:t)'),dict(t=t)).scalar() for t in TABLES)


def clean(values,key,label,maximum=150,required=True):
    value=(values.get(key) or '').strip()
    if (required and not value) or len(value)>maximum:abort(400,description=label+' is required and must contain at most '+str(maximum)+' characters.' if required else label+' is too long.')
    return value


def day(value,label):
    try:
        result=date.fromisoformat(str(value))
        if result.isoformat()!=str(value):raise ValueError()
        return result
    except (ValueError,TypeError):abort(400,description='Enter a valid '+label+'.')


def money(value,label):
    try:
        raw=str(value)
        if not re.fullmatch(r'[0-9]+(?:\.[0-9]{1,2})?',raw):raise ValueError()
        result=Decimal(raw)
        if not result.is_finite() or not Decimal('0')<result<=Decimal('999999999999.99'):raise ValueError()
        return result.quantize(Decimal('.01'))
    except (ValueError,InvalidOperation):abort(400,description=label+' must be a positive rand amount with at most two decimal places.')


def identifier(value):
    try:
        number=int(value)
        if number<=0:raise ValueError()
        return number
    except (TypeError,ValueError):abort(400,description='Select a valid record.')


def accounts(p):
    return [dict(r) for r in db.session.execute(text('SELECT id,account_number,ca_contract_number,ca_agreement_amount,ca_installments_raised,ca_installment_amount,ca_amount_owing,ca_remaining_periods FROM bil_muni_account WHERE property_id=:p ORDER BY account_number,id'),dict(p=p)).mappings()]


def account(p,account_id):
    row=db.session.execute(text('SELECT id,account_number FROM bil_muni_account WHERE id=:a AND property_id=:p FOR SHARE'),dict(a=identifier(account_id),p=p)).mappings().first()
    if row is None:abort(403,description='Select a municipal account belonging to this property.')
    return row


def record(p,arrangement_id,lock=False):
    row=db.session.execute(text('SELECT a.* FROM bil_municipal_arrangement a JOIN bil_muni_account m ON m.id=a.account_id AND m.property_id=a.property_id WHERE a.property_id=:p AND a.id=:id'+(' FOR UPDATE OF a' if lock else '')),dict(p=p,id=identifier(arrangement_id))).mappings().first()
    if row is None:abort(404)
    return dict(row)


def payments(arrangement_id):
    return [dict(r) for r in db.session.execute(text('SELECT * FROM bil_municipal_arrangement_payment WHERE arrangement_id=:id ORDER BY payment_date,id'),dict(id=arrangement_id)).mappings()]


def schedule(row,paid=Decimal('0.00'),on=None):
    on=on or today();result=[];remaining=row['original_amount'];allocated=paid;first=row['first_payment_date']
    for i in range(row['instalments']):
        if row['frequency'] in ('weekly','fortnightly'):due=first+timedelta(days=i*(7 if row['frequency']=='weekly' else 14))
        else:
            offset=first.year*12+first.month-1+i*(1 if row['frequency']=='monthly' else 3)
            year,month=divmod(offset,12);month+=1
            due=date(year,month,min(first.day,calendar.monthrange(year,month)[1]))
        amount=min(row['instalment_amount'],remaining);received=min(amount,allocated)
        remaining-=amount;allocated-=received;balance=amount-received
        result.append(dict(number=i+1,due_date=due,amount=amount,paid=received,outstanding=balance,status='Paid' if balance==0 else 'Overdue' if due<on else 'Due' if due==on else 'Upcoming'))
    return result


def summary(row):
    paid=sum((r['amount'] for r in payments(row['id'])),Decimal('0.00'))
    return dict(paid=paid,outstanding=row['original_amount']-paid,status='Completed' if paid==row['original_amount'] else STATUSES[row['status']],schedule=schedule(row,paid))


def begin(p,values,kind,arrangement_id):
    scope(p)
    if not ready():abort(503,description='Municipal Payment Arrangements is temporarily unavailable. Please contact the administrator.')
    try:key=str(uuid.UUID(values.get('request_key','')))
    except (ValueError,TypeError,AttributeError):abort(400,description='Reopen the form before saving.')
    digest=hashlib.sha256(json.dumps(dict(property=p,kind=kind,arrangement=arrangement_id,values={k:v for k,v in values.items() if k not in ('csrf_token','request_key')}),sort_keys=True,default=str).encode()).hexdigest()
    if db.session.execute(text('SHOW transaction_isolation')).scalar()!='read committed':abort(503,description='Arrangement saving requires PostgreSQL READ COMMITTED isolation.')
    db.session.execute(text("SET LOCAL lock_timeout = '5s'"))
    db.session.execute(text('SELECT pg_advisory_xact_lock(hashtextextended(:key,0))'),dict(key='pm-municipal-arrangement:'+key))
    existing=db.session.execute(text('SELECT e.*,a.property_id FROM bil_municipal_arrangement_event e JOIN bil_municipal_arrangement a ON a.id=e.arrangement_id WHERE e.request_key=CAST(:key AS uuid)'),dict(key=key)).mappings().first()
    if existing:
        if existing['property_id']!=p or existing['actor_id']!=current_user.id or existing['payload_hash']!=digest:abort(409,description='This submission was already used with different information. Reopen the form.')
        return key,digest,existing['arrangement_id']
    return key,digest,None


def event(arrangement_id,kind,key,digest,before,after,note=''):
    db.session.execute(text('INSERT INTO bil_municipal_arrangement_event (arrangement_id,kind,actor_id,request_key,payload_hash,before_data,after_data,note) VALUES (:id,:kind,:actor,CAST(:key AS uuid),:hash,CAST(:before AS jsonb),CAST(:after AS jsonb),:note)'),dict(id=arrangement_id,kind=kind,actor=current_user.id,key=key,hash=digest,before=json.dumps(before,default=str) if before is not None else None,after=json.dumps(after,default=str),note=note))


def create(p,values):
    key,digest,replay=begin(p,values,'created',None)
    if replay:return replay
    acc=account(p,values.get('account_id'))
    reference=clean(values,'reference','Municipal arrangement reference')
    agreement=day(values.get('agreement_date'),'agreement date');first=day(values.get('first_payment_date'),'first payment date')
    amount=money(values.get('original_amount'),'Original arrangement amount');instalment=money(values.get('instalment_amount'),'Instalment amount')
    count=identifier(values.get('instalments'));frequency=values.get('frequency')
    if count>600 or frequency not in FREQUENCIES or first<agreement:abort(400,description='Check the instalment count (1-600), payment frequency and first payment date.')
    if not instalment*(count-1)<amount<=instalment*count:abort(400,description='The amount and number of instalments must cover the arrangement exactly. Only the final instalment may be smaller.')
    data=dict(property_id=p,account_id=acc['id'],reference=reference,reference_key=reference.casefold(),agreement_date=agreement,original_amount=amount,instalment_amount=instalment,instalments=count,frequency=frequency,first_payment_date=first,document_reference=clean(values,'document_reference','Supporting document reference',1000,False),actor=current_user.id)
    try:schedule(data)
    except (ValueError,OverflowError):abort(400,description='The instalment schedule exceeds the supported date range.')
    fields=[k for k in data if k!='actor']
    arrangement_id=db.session.execute(text('INSERT INTO bil_municipal_arrangement ('+','.join(fields)+',created_by,modified_by) VALUES ('+','.join(':'+k for k in fields)+',:actor,:actor) RETURNING id'),data).scalar_one()
    event(arrangement_id,'created',key,digest,None,data)
    return arrangement_id


def record_payment(p,arrangement_id,values):
    key,digest,replay=begin(p,values,'payment',arrangement_id)
    if replay:return replay
    row=record(p,arrangement_id,True)
    if row['status']=='cancelled':abort(409,description='Payments cannot be allocated to a cancelled arrangement.')
    source=None
    if values.get('source_payment_id'):
        source=db.session.execute(text('SELECT * FROM bil_muni_payment WHERE id=:id AND account_id=:a FOR SHARE'),dict(id=identifier(values['source_payment_id']),a=row['account_id'])).mappings().first()
        if source is None:abort(403,description='This municipal payment does not belong to the arrangement account.')
    amount=money(str(source['amount']) if source else values.get('amount'),'Payment amount')
    paid_date=day(str(source['payment_date']) if source else values.get('payment_date'),'payment date')
    if not row['agreement_date']<=paid_date<=today():abort(400,description='Payment date must be on or after the agreement date and cannot be in the future.')
    ref=(source['reference'] or '').strip() if source else ''
    reference=ref or clean(values,'payment_reference','Payment reference')
    if len(reference)>150:abort(400,description='Payment reference is too long.')
    paid=sum((r['amount'] for r in payments(arrangement_id)),Decimal('0.00'))
    if amount>row['original_amount']-paid:abort(400,description='This payment exceeds the outstanding arrangement balance.')
    data=dict(arrangement=arrangement_id,account=row['account_id'],source=source['id'] if source else None,day=paid_date,amount=amount,ref=reference,key=reference.casefold(),actor=current_user.id)
    db.session.execute(text('INSERT INTO bil_municipal_arrangement_payment (arrangement_id,account_id,source_payment_id,payment_date,amount,reference,reference_key,recorded_by) VALUES (:arrangement,:account,:source,:day,:amount,:ref,:key,:actor)'),data)
    db.session.execute(text('UPDATE bil_municipal_arrangement SET version=version+1,modified_by=:actor,modified_at=statement_timestamp() WHERE id=:id'),dict(actor=current_user.id,id=arrangement_id))
    event(arrangement_id,'payment',key,digest,dict(paid=paid),dict(payment=data,source_snapshot=dict(source) if source else None,paid=paid+amount,outstanding=row['original_amount']-paid-amount))
    return arrangement_id


def update_status(p,arrangement_id,values):
    key,digest,replay=begin(p,values,'status',arrangement_id)
    if replay:return replay
    row=record(p,arrangement_id,True)
    if str(row['version'])!=values.get('version'):abort(409,description='The arrangement changed. Review the latest details before saving; your entered values are retained.')
    status=values.get('status')
    if status not in STATUSES:abort(400,description='Select a valid status.')
    document=clean(values,'document_reference','Supporting document reference',1000,False)
    note=clean(values,'note','History note',5000,False)
    db.session.execute(text('UPDATE bil_municipal_arrangement SET status=:status,document_reference=:doc,version=version+1,modified_by=:actor,modified_at=statement_timestamp() WHERE id=:id'),dict(status=status,doc=document,actor=current_user.id,id=arrangement_id))
    event(arrangement_id,'status',key,digest,dict(status=row['status'],document_reference=row['document_reference']),dict(status=status,document_reference=document),note)
    return arrangement_id


def page(p,arrangement_id=None):
    scope(p)
    from .lease_management import render_pm_template
    prop=db.session.execute(text('SELECT id,name,address FROM bil_property WHERE id=:p'),dict(p=p)).mappings().one()
    available=ready();error=None;status_code=200;values=request.form.to_dict() if request.method=='POST' else {}
    utility_month=request.values.get('billing_month','')
    if request.method=='POST':
        try:
            action=values.get('action')
            if arrangement_id is None and action=='create':saved=create(p,values)
            elif arrangement_id is not None and action=='payment':saved=record_payment(p,arrangement_id,values)
            elif arrangement_id is not None and action=='status':saved=update_status(p,arrangement_id,values)
            else:abort(400,description='Select a valid arrangement action.')
            db.session.commit();flash('Municipal payment arrangement saved.' if action!='payment' else 'Arrangement payment recorded. No ledger transaction was created.','success')
            return redirect(url_for('billing_bp.municipal_arrangement_detail',property_id=p,arrangement_id=saved,billing_month=utility_month))
        except HTTPException as exc:db.session.rollback();error=exc.description;status_code=exc.code
        except IntegrityError:db.session.rollback();error='This arrangement or payment reference is already recorded. Review the existing records before retrying.';status_code=409
        except SQLAlchemyError:
            db.session.rollback();current_app.logger.exception('Municipal arrangement save failed');error='Saving failed; no changes were committed. Please contact the administrator.';status_code=503
    rows=[];selected=None;details=None;history=[];sources=[]
    if available:
        rows=[dict(r) for r in db.session.execute(text('SELECT a.*,m.account_number FROM bil_municipal_arrangement a JOIN bil_muni_account m ON m.id=a.account_id AND m.property_id=a.property_id WHERE a.property_id=:p ORDER BY a.created_at DESC,a.id DESC'),dict(p=p)).mappings()]
        for row in rows:row.update(summary(row))
        if arrangement_id is not None:
            selected=record(p,arrangement_id);details=summary(selected)
            history=[dict(r) for r in db.session.execute(text('SELECT * FROM bil_municipal_arrangement_event WHERE arrangement_id=:id ORDER BY recorded_at,id'),dict(id=arrangement_id)).mappings()]
            catalog_exists=db.session.execute(text("SELECT to_regclass('bil_muni_payment')")).scalar()
            if catalog_exists:
                sources=[dict(r) for r in db.session.execute(text('SELECT p.* FROM bil_muni_payment p WHERE p.account_id=:a AND NOT EXISTS(SELECT 1 FROM bil_municipal_arrangement_payment x WHERE x.source_payment_id=p.id) ORDER BY p.payment_date DESC,p.id DESC'),dict(a=selected['account_id'])).mappings()]
    return render_pm_template('program_billing/municipal_arrangements.html',property=prop,utility_month=utility_month,available=available,error=error,rows=rows,selected=selected,details=details,history=history,sources=sources,payments=payments(arrangement_id) if selected else [],accounts=accounts(p),values=values,show_form=request.args.get('new')=='1' or (request.method=='POST' and arrangement_id is None),frequencies=FREQUENCIES,statuses=STATUSES,request_key=values.get('request_key') or str(uuid.uuid4())),status_code
