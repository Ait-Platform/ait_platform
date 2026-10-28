"""PM fixed monthly rent. No legacy materializer, METSOA or shared Debtor calls."""
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


def require_schema():
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


def for_account(property_id,tenant_id):
    require_schema()
    posting.authorise(property_id,tenant_id)
    row=db.session.execute(text("""SELECT * FROM bil_tenant_recurring
        WHERE pm_rent_schedule AND pm_property_id=:p AND tenant_id=:t AND pm_manager_id=:m"""),
        dict(p=property_id,t=tenant_id,m=current_user.id)).mappings().first()
    return dict(row) if row else None


def form_token(property_id,tenant_id,schedule):
    kind='fixed:add' if schedule is None else 'fixed:'+str(schedule['id'])+':'+str(schedule['pm_version'])
    return posting.token(property_id,tenant_id,kind)


def parse(form):
    raw=form.get('amount','').strip()
    if not re.fullmatch(r'[0-9]{1,8}(?:\.[0-9]{1,2})?',raw) or Decimal(raw)<=0:
        raise ValueError('Enter a positive rent amount with at most two decimals, up to R99,999,999.99.')
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
    require_schema()
    posting.authorise(property_id,tenant_id)
    amount,start,end,day,active=parse(form)
    try:
        scope=posting.signer().loads(form.get('posting_key',''))
        if (not isinstance(scope,list) or len(scope)!=5 or
            scope[:3]!=[current_user.id,property_id,tenant_id] or not isinstance(scope[3],str) or
            not re.fullmatch(r'fixed:(?:add|[0-9]+:[0-9]+)',scope[3]) or
            not re.fullmatch('[a-f0-9]{32}',scope[4])):
            raise BadSignature('scope')
    except (BadSignature,TypeError):
        raise ValueError('Invalid Fixed Charges form. Open Fixed Charges again.')
    digest=hashlib.sha256(json.dumps([str(amount),str(start),str(end),day,active]).encode()).hexdigest()
    # One definition per property/tenant, with optimistic version and persistent retry keys.
    lock=int.from_bytes(hashlib.sha256(('PMFIXED:'+str(property_id)+':'+str(tenant_id)).encode()).digest()[:8],'big',signed=True)
    db.session.execute(text('SELECT pg_advisory_xact_lock(:key)'),dict(key=lock))
    row=db.session.execute(text("""SELECT * FROM bil_tenant_recurring
        WHERE pm_rent_schedule AND pm_property_id=:p AND tenant_id=:t FOR UPDATE"""),
        dict(p=property_id,t=tenant_id)).mappings().first()
    if row and row['pm_manager_id']!=current_user.id:abort(404)
    if row and scope[4] in (row['pm_create_key'],row['pm_last_key']):
        expected=row['pm_create_hash'] if scope[4]==row['pm_create_key'] else row['pm_last_hash']
        if digest!=expected:abort(409,description='This Fixed Charges form was already saved with different details.')
        db.session.commit()
        return False
    params=dict(t=tenant_id,p=property_id,m=current_user.id,amount=amount,start=start,end=end,
        day=day,active=active,key=scope[4],digest=digest)
    if row is None:
        if scope[3]!='fixed:add':abort(409,description='The rent schedule changed. Open Fixed Charges again.')
        db.session.execute(text("""INSERT INTO bil_tenant_recurring
            (tenant_id,description,kind,amount,day_of_month,is_active,
             pm_rent_schedule,pm_active,pm_property_id,pm_manager_id,pm_start_date,pm_end_date,
             pm_version,pm_create_key,pm_create_hash,pm_last_key,pm_last_hash)
            VALUES(:t,'Rent Due','charge',:amount,:day,0,true,:active,:p,:m,:start,:end,1,:key,:digest,:key,:digest)"""),params)
    else:
        if scope[3]!='fixed:'+str(row['id'])+':'+str(row['pm_version']):
            abort(409,description='The rent schedule changed. Reopen Fixed Charges before saving.')
        params['id']=row['id']
        db.session.execute(text("""UPDATE bil_tenant_recurring SET amount=:amount,day_of_month=:day,
            pm_active=:active,pm_start_date=:start,pm_end_date=:end,pm_version=pm_version+1,
            pm_last_key=:key,pm_last_hash=:digest WHERE id=:id"""),params)
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
    require_schema()
    row=db.session.execute(text('SELECT * FROM bil_tenant_recurring WHERE id=:id AND pm_rent_schedule FOR UPDATE'),
        dict(id=schedule_id)).mappings().first()
    if row is None or not row['pm_active']:
        db.session.commit()
        return False
    posting.authorise(row['pm_property_id'],row['tenant_id'],manager_id=row['pm_manager_id'])
    if (due>today() or due<row['pm_start_date'] or due>row['pm_end_date'] or
            due!=scheduled_date(due.year,due.month,row['day_of_month'])):
        db.session.commit()
        return False
    month=due.strftime('%Y-%m')
    exists=db.session.execute(text("""SELECT EXISTS(SELECT 1 FROM bil_tenant_ledger
        WHERE tenant_id=:t AND rent_property_id=:p AND month=:month AND rent_schedule_id IS NOT NULL)"""),
        dict(t=row['tenant_id'],p=row['pm_property_id'],month=month)).scalar()
    if exists:
        db.session.commit()
        return False
    key=hashlib.sha256(('PMRENT:'+str(row['tenant_id'])+':'+str(row['pm_property_id'])+':'+month).encode()).hexdigest()[:32]
    digest=hashlib.sha256(json.dumps([row['id'],str(due),str(row['amount'])]).encode()).hexdigest()
    entry_id=posting.append_entry(row['tenant_id'],due,'Rent Due','charge',row['amount'],key,digest)
    db.session.execute(text("""UPDATE bil_tenant_ledger SET rent_schedule_id=:schedule,rent_property_id=:p
        WHERE id=:id"""),dict(schedule=row['id'],p=row['pm_property_id'],id=entry_id))
    db.session.commit()
    return True


def run_due():
    """Catch up eligible months, bounded per run. No request/GET materialisation."""
    require_schema()
    schedules=db.session.execute(text("""SELECT * FROM bil_tenant_recurring
        WHERE pm_rent_schedule AND pm_active ORDER BY id""")).mappings().all()
    db.session.commit()
    posted=0;failed=0
    for schedule in schedules:
        count=0
        try:
            recorded=set(db.session.execute(text("""SELECT month FROM bil_tenant_ledger
                WHERE tenant_id=:t AND rent_property_id=:p AND rent_schedule_id IS NOT NULL"""),
                dict(t=schedule['tenant_id'],p=schedule['pm_property_id'])).scalars())
            db.session.commit()
            for due in due_dates(schedule,today()):
                if due.strftime('%Y-%m') in recorded:continue
                if post_due(schedule['id'],due):
                    count+=1;posted+=1
                if count>=120:break
        except Exception as exc:
            db.session.rollback()
            logging.getLogger(__name__).error('PM rent schedule %s failed (%s)',schedule['id'],type(exc).__name__)
            failed+=1
    return dict(posted=posted,failed=failed)
