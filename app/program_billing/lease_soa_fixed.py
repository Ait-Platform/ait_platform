"""PM fixed-charge definitions. No legacy materializer, METSOA or shared Debtor calls."""
import calendar
import hashlib
import json
import logging
import re
from datetime import date
from decimal import Decimal
from flask import abort
from flask_login import current_user
from itsdangerous import BadSignature
from sqlalchemy import inspect, text
from app.extensions import db
from . import lease_soa_posting as posting
from .lease_soa_dates import today

CHARGE_TYPES={'rent':'Rent Due','water':'Water','electricity':'Electricity',
    'sewerage':'Sewerage','refuse':'Refuse Removal','other_utilities':'Other Utilities'}


def require_schema(charge_type='rent'):
    posting.require_posting_schema()
    inspector=inspect(db.session.connection())
    if not inspector.has_table('bil_tenant_recurring'):
        abort(503,description='Fixed Charges requires migration pm_soa_003 and the existing PM recurring table.')
    columns={c['name']:c for c in inspector.get_columns('bil_tenant_recurring')}
    required={'pm_rent_schedule','pm_active','pm_property_id','pm_manager_id','pm_start_date',
        'pm_end_date','pm_version','pm_create_key','pm_create_hash','pm_last_key','pm_last_hash'}
    ledger={c['name'] for c in inspector.get_columns('bil_tenant_ledger')}
    if (not required<=columns.keys() or not {'rent_schedule_id','rent_property_id'}<=ledger or
            getattr(columns.get('amount',{}).get('type'),'scale',None)!=2 or
            'uq_pm_recurring_account' not in {i['name'] for i in inspector.get_indexes('bil_tenant_recurring') if i['unique']} or
            'uq_pm_ledger_monthly_rent' not in {i['name'] for i in inspector.get_indexes('bil_tenant_ledger') if i['unique']}):
        abort(503,description='Fixed Charges requires migration pm_soa_003. No financial transaction was posted.')
    utilities={'pm_charge_type','pm_fixed_agreed'}<=columns.keys() and 'fixed_charge_type' in ledger
    if charge_type!='rent' and not utilities:
        abort(503,description='Utility Charges requires migration pm_soa_004. No financial transaction was posted.')
    return utilities


def list_for_account(property_id,tenant_id):
    require_schema()
    posting.authorise(property_id,tenant_id)
    return [dict(row) for row in db.session.execute(text("""SELECT * FROM bil_tenant_recurring
        WHERE pm_rent_schedule AND pm_property_id=:p AND tenant_id=:t AND pm_manager_id=:m ORDER BY id"""),
        dict(p=property_id,t=tenant_id,m=current_user.id)).mappings()]


def for_account(property_id,tenant_id,charge_type='rent'):
    return next((r for r in list_for_account(property_id,tenant_id) if
        (r.get('pm_charge_type') or 'rent')==charge_type),None)


def form_token(property_id,tenant_id,schedule,group='rent'):
    kind='fixed:add:'+group if schedule is None else 'fixed:'+str(schedule['id'])+':'+str(schedule['pm_version'])
    return posting.token(property_id,tenant_id,kind)


def parse(form):
    raw=form.get('amount','').strip()
    if not re.fullmatch(r'[0-9]{1,8}(?:\.[0-9]{1,2})?',raw) or Decimal(raw)<=0:
        raise ValueError('Enter a positive amount with at most two decimals, up to R99,999,999.99.')
    amount=Decimal(raw).quantize(Decimal('0.01'))
    try:
        start=date.fromisoformat(form.get('start_date',''))
        end=date.fromisoformat(form.get('end_date',''))
        if not all(re.fullmatch(r'\d{4}-\d{2}-\d{2}',form.get(k,'')) for k in ('start_date','end_date')) or end<start:
            raise ValueError()
        day=int(form.get('day_of_month','1'))
        if not 1<=day<=31:raise ValueError()
    except (ValueError,TypeError):
        raise ValueError('Enter valid start/end dates and a monthly posting day from 1 to 31.')
    status=form.get('status','')
    if status not in ('active','inactive'):raise ValueError('Choose Active or Inactive.')
    return amount,start,end,day,status=='active'


def save(property_id,tenant_id,form):
    charge_type=form.get('charge_type','rent')
    if charge_type not in CHARGE_TYPES:raise ValueError('Select an available charge description.')
    utilities=require_schema(charge_type)
    posting.authorise(property_id,tenant_id)
    agreed=form.get('fixed_agreed')=='1'
    if charge_type!='rent' and not agreed:
        raise ValueError('Only an explicitly agreed fixed monthly utility amount may be scheduled. Variable utilities cannot be automated.')
    amount,start,end,day,active=parse(form)
    try:
        scope=posting.signer().loads(form.get('posting_key',''))
        if (not isinstance(scope,list) or len(scope)!=5 or
            scope[:3]!=[current_user.id,property_id,tenant_id] or not isinstance(scope[3],str) or
            not re.fullmatch(r'fixed:(?:add(?::rent|:utility)?|[0-9]+:[0-9]+)',scope[3]) or
            not re.fullmatch('[a-f0-9]{32}',scope[4])):
            raise BadSignature('scope')
        if scope[3].startswith('fixed:add') and (scope[3]=='fixed:add:utility')!=(charge_type!='rent'):
            raise BadSignature('group')
    except (BadSignature,TypeError):
        raise ValueError('Invalid Fixed Charges form. Open Add Charge or Edit again.')
    payload=[str(amount),str(start),str(end),day,active]
    digest=hashlib.sha256(json.dumps([charge_type,agreed]+payload).encode()).hexdigest()
    legacy_digest=hashlib.sha256(json.dumps(payload).encode()).hexdigest() if charge_type=='rent' else None
    lock=int.from_bytes(hashlib.sha256(('PMFIXED:'+str(property_id)+':'+str(tenant_id)).encode()).digest()[:8],'big',signed=True)
    db.session.execute(text('SELECT pg_advisory_xact_lock(:key)'),dict(key=lock))
    replay=db.session.execute(text("""SELECT * FROM bil_tenant_recurring
        WHERE pm_rent_schedule AND pm_property_id=:p AND tenant_id=:t
          AND (pm_create_key=:key OR pm_last_key=:key)"""),
        dict(p=property_id,t=tenant_id,key=scope[4])).mappings().first()
    if replay:
        if replay['pm_manager_id']!=current_user.id:abort(404)
        expected=replay['pm_create_hash'] if scope[4]==replay['pm_create_key'] else replay['pm_last_hash']
        if expected not in (digest,legacy_digest):
            abort(409,description='This Fixed Charges form was already saved with different details.')
        db.session.commit();return False
    params=dict(t=tenant_id,p=property_id,m=current_user.id,amount=amount,start=start,end=end,
        day=day,active=active,key=scope[4],digest=digest,charge_type=charge_type,
        agreed=agreed,description=CHARGE_TYPES[charge_type])
    if scope[3].startswith('fixed:add'):
        filter_type=" AND COALESCE(pm_charge_type,'rent')=:charge_type" if utilities else ''
    else:
        params['id']=int(scope[3].split(':')[1]);filter_type=' AND id=:id'
    row=db.session.execute(text("""SELECT * FROM bil_tenant_recurring
        WHERE pm_rent_schedule AND pm_property_id=:p AND tenant_id=:t"""+filter_type+' FOR UPDATE'),params).mappings().first()
    if row and row['pm_manager_id']!=current_user.id:abort(404)
    if row and (row.get('pm_charge_type') or 'rent')!=charge_type:
        raise ValueError('An existing charge description cannot be changed. Use Add Charge for a different category.')
    if row and scope[4] in (row['pm_create_key'],row['pm_last_key']):
        expected=row['pm_create_hash'] if scope[4]==row['pm_create_key'] else row['pm_last_hash']
        if expected not in (digest,legacy_digest):
            abort(409,description='This Fixed Charges form was already saved with different details.')
        db.session.commit();return False
    if row is None:
        if not scope[3].startswith('fixed:add'):abort(404)
        extra_columns=',pm_charge_type,pm_fixed_agreed' if utilities else ''
        extra_values=',:charge_type,:agreed' if utilities else ''
        db.session.execute(text("""INSERT INTO bil_tenant_recurring
            (tenant_id,description,kind,amount,day_of_month,is_active,
             pm_rent_schedule,pm_active,pm_property_id,pm_manager_id,pm_start_date,pm_end_date,
             pm_version,pm_create_key,pm_create_hash,pm_last_key,pm_last_hash"""+extra_columns+""")
            VALUES(:t,:description,'charge',:amount,:day,0,true,:active,:p,:m,:start,:end,1,:key,:digest,:key,:digest"""+extra_values+')'),params)
    else:
        if scope[3]!='fixed:'+str(row['id'])+':'+str(row['pm_version']):
            abort(409,description='The charge schedule changed. Reopen Edit before saving.')
        params['id']=row['id']
        extra=',pm_fixed_agreed=:agreed' if utilities else ''
        db.session.execute(text("""UPDATE bil_tenant_recurring SET amount=:amount,day_of_month=:day,
            pm_active=:active,pm_start_date=:start,pm_end_date=:end,pm_version=pm_version+1,
            pm_last_key=:key,pm_last_hash=:digest"""+extra+' WHERE id=:id'),params)
    db.session.commit()
    return True


def scheduled_date(year,month,day):
    return date(year,month,min(day,calendar.monthrange(year,month)[1]))


def due_dates(schedule,as_of):
    cursor=schedule['pm_start_date'].replace(day=1)
    end=min(as_of,schedule['pm_end_date'])
    while cursor<=end:
        due=scheduled_date(cursor.year,cursor.month,schedule['day_of_month'])
        if schedule['pm_start_date']<=due<=end:yield due
        if cursor.year==9999 and cursor.month==12:return
        cursor=date(cursor.year+1,1,1) if cursor.month==12 else date(cursor.year,cursor.month+1,1)


def post_due(schedule_id,due):
    """Background-only entry; dates and manager scope come from the locked schedule."""
    utilities=require_schema()
    row=db.session.execute(text('SELECT * FROM bil_tenant_recurring WHERE id=:id AND pm_rent_schedule FOR UPDATE'),
        dict(id=schedule_id)).mappings().first()
    if row is None or not row['pm_active']:
        db.session.commit()
        return False
    charge_type=row.get('pm_charge_type') or 'rent'
    if charge_type not in CHARGE_TYPES or row['description']!=CHARGE_TYPES.get(charge_type) or row['amount']<=0:
        abort(409,description='The fixed-charge definition is invalid. No financial transaction was posted.')
    if charge_type!='rent':
        require_schema(charge_type)
        if not row.get('pm_fixed_agreed'):
            abort(409,description='Variable or unagreed utility charges cannot be posted automatically.')
    posting.authorise(row['pm_property_id'],row['tenant_id'],manager_id=row['pm_manager_id'])
    if (due>today() or due<row['pm_start_date'] or due>row['pm_end_date'] or
            due!=scheduled_date(due.year,due.month,row['day_of_month'])):
        db.session.commit()
        return False
    month=due.strftime('%Y-%m')
    type_filter=" AND COALESCE(fixed_charge_type,'rent')=:charge_type" if utilities else ''
    exists=db.session.execute(text("""SELECT EXISTS(SELECT 1 FROM bil_tenant_ledger
        WHERE tenant_id=:t AND rent_property_id=:p AND month=:month AND rent_schedule_id IS NOT NULL"""+type_filter+')'),
        dict(t=row['tenant_id'],p=row['pm_property_id'],month=month,charge_type=charge_type)).scalar()
    if exists:
        db.session.commit()
        return False
    key_source=('PMRENT:' if charge_type=='rent' else 'PMFIXED:'+charge_type+':')
    key=hashlib.sha256((key_source+str(row['tenant_id'])+':'+str(row['pm_property_id'])+':'+month).encode()).hexdigest()[:32]
    digest=hashlib.sha256(json.dumps([row['id'],str(due),str(row['amount'])]).encode()).hexdigest()
    entry_id=posting.append_entry(row['tenant_id'],due,CHARGE_TYPES[charge_type],'charge',row['amount'],key,digest)
    extra=',fixed_charge_type=:charge_type' if utilities else ''
    db.session.execute(text("""UPDATE bil_tenant_ledger SET rent_schedule_id=:schedule,rent_property_id=:p"""+extra+"""
        WHERE id=:id"""),dict(schedule=row['id'],p=row['pm_property_id'],id=entry_id,charge_type=charge_type))
    db.session.commit()
    return True


def run_due():
    """Catch up eligible months, bounded per run. No request/GET materialisation."""
    utilities=require_schema()
    schedules=db.session.execute(text("""SELECT * FROM bil_tenant_recurring
        WHERE pm_rent_schedule AND pm_active ORDER BY id""")).mappings().all()
    db.session.commit()
    posted=0;failed=0
    for schedule in schedules:
        charge_type=schedule.get('pm_charge_type') or 'rent'
        count=0
        try:
            type_filter=" AND COALESCE(fixed_charge_type,'rent')=:charge_type" if utilities else ''
            recorded=set(db.session.execute(text("""SELECT month FROM bil_tenant_ledger
                WHERE tenant_id=:t AND rent_property_id=:p AND rent_schedule_id IS NOT NULL"""+type_filter),
                dict(t=schedule['tenant_id'],p=schedule['pm_property_id'],charge_type=charge_type)).scalars())
            db.session.commit()
            for due in due_dates(schedule,today()):
                if due.strftime('%Y-%m') in recorded:continue
                if post_due(schedule['id'],due):
                    count+=1;posted+=1
                if count>=120:break
        except Exception as exc:
            db.session.rollback()
            logging.getLogger(__name__).error('PM fixed-charge schedule %s failed (%s)',schedule['id'],type(exc).__name__)
            failed+=1
    return dict(posted=posted,failed=failed)
