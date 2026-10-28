"""Lease Kit rent projection using the existing PM recurring schedule and ledger."""
import calendar
import hashlib
import json
import re
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from flask import abort
from flask_login import current_user
from sqlalchemy import text
from . import lease_soa_fixed as fixed
from . import lease_soa_posting as posting
from .lease_certificate import selected_tenants
from .lease_soa_dates import today


def month_date(start, offset):
    year, month=divmod(start.year*12+start.month-1+offset,12)
    return date(year,month+1,min(start.day,calendar.monthrange(year,month+1)[1]))


def plan(property_id,tenant_id):
    posting.authorise(property_id,tenant_id)
    drafts=posting.db.session.execute(text("""SELECT id,version,items FROM bil_lease_draft
        WHERE property_id=:p AND manager_id=:m AND status='draft'
        ORDER BY updated_at DESC,id DESC"""),dict(p=property_id,m=current_user.id)).mappings()
    draft=next((d for d in drafts if any(str(p.get('tenant_id'))==str(tenant_id)
        for p in selected_tenants(d['items']))),None)
    if draft is None:raise ValueError('No saved Lease Kit draft selects this registered tenant.')
    items=draft['items'];period=items.get('_period') or {}
    def value(key,legacy):return period.get(key) or (items.get(legacy) or {}).get('value','')
    try:
        start=date.fromisoformat(str(value('commencement_date','s1-18')))
        match=re.fullmatch(r'(\d+)\s*(months?|years?)?',str(value('duration_months','s1-17')).strip(),re.I)
        months=int(match[1])*(12 if (match[2] or '').lower().startswith('year') else 1) if match else 0
        amount=Decimal(str((items.get('_amounts') or {}).get('monthly_rent','')))
        if not 1<=months<=1200 or not amount.is_finite() or not Decimal('0')<amount<=Decimal('99999999.99') or amount!=amount.quantize(Decimal('.01')):raise ValueError()
        end=month_date(start,months)-timedelta(days=1)
    except (ValueError,InvalidOperation,OverflowError):
        raise ValueError('Save a valid monthly rent, commencement date and duration in Lease Kit first.')
    schedule=fixed.for_account(property_id,tenant_id)
    assigned=bool(schedule and schedule['amount']==amount and schedule['pm_start_date']==start and
        schedule['pm_end_date']==end and schedule['day_of_month']==start.day and schedule['pm_active'])
    snapshot=hashlib.sha256(json.dumps([str(draft['id']),draft['version'],str(amount),str(start),months,
        schedule['pm_version'] if schedule else None]).encode()).hexdigest()
    key=posting.token(property_id,tenant_id,'rent-plan:'+snapshot)
    utilities=fixed.require_schema()
    category=" AND COALESCE(fixed_charge_type,'rent')='rent'" if utilities else ''
    posted={r['month']:r['ref'] for r in posting.db.session.execute(text("""SELECT month,ref FROM bil_tenant_ledger
        WHERE tenant_id=:t AND rent_property_id=:p AND rent_schedule_id IS NOT NULL"""+category),dict(t=tenant_id,p=property_id)).mappings()}
    rows=[]
    for offset in range(months):
        due=month_date(start,offset);ref=posted.get(due.strftime('%Y-%m'))
        rows.append(dict(month=due.strftime('%B %Y'),due=due,amount=amount,ref=ref,
            status='Posted' if ref else 'Assigned' if assigned else 'Not assigned',can_post=assigned and not ref and due<=today()))
    return dict(draft_id=str(draft['id']),version=draft['version'],amount=amount,start=start,end=end,
        rows=rows,total=amount*months,assigned=assigned,schedule=schedule,key=key,snapshot=snapshot)


def act(property_id,tenant_id,form):
    from itsdangerous import BadSignature
    posting.authorise(property_id,tenant_id)
    lock=int.from_bytes(hashlib.sha256(('PMFIXED:'+str(property_id)+':'+str(tenant_id)).encode()).digest()[:8],'big',signed=True)
    posting.db.session.execute(text('SELECT pg_advisory_xact_lock(:key)'),dict(key=lock))
    posting.db.session.execute(text('SELECT id FROM bil_tenant_recurring WHERE pm_rent_schedule AND pm_property_id=:p AND tenant_id=:t FOR UPDATE'),dict(p=property_id,t=tenant_id)).all()
    result=plan(property_id,tenant_id)
    try:
        scope=posting.signer().loads(form.get('posting_key',''))
        if not isinstance(scope,list) or len(scope)!=5 or scope[:4]!=[current_user.id,property_id,tenant_id,'rent-plan:'+result['snapshot']]:raise BadSignature('scope')
    except (BadSignature,TypeError):
        abort(409,description='The lease or schedule changed. Reload Fixed Charges before assigning or posting.')
    if form.get('rent_action')=='assign':
        if result['assigned']:posting.db.session.commit();return 'Rent schedule is already assigned.'
        fixed.save(property_id,tenant_id,dict(charge_type='rent',amount=str(result['amount']),
            start_date=str(result['start']),end_date=str(result['end']),day_of_month=str(result['start'].day),status='active',
            posting_key=fixed.form_token(property_id,tenant_id,result['schedule'])))
        return 'Monthly rent schedule assigned. No charges were posted.'
    if form.get('rent_action')!='post':abort(400)
    try:due=date.fromisoformat(form.get('due',''))
    except ValueError:abort(400)
    if not result['assigned']:abort(409,description='Assign the current Lease Kit rent schedule before posting.')
    if not any(row['due']==due for row in result['rows']) or due>today():
        abort(400,description='Only a due month in this lease schedule may be posted.')
    created=fixed.post_due(result['schedule']['id'],due)
    return 'Rent posted to Tenant Ledger.' if created else 'This month was already posted; no duplicate was recorded.'
