"""Read-only adapters for existing reports in the central Utilities Output page."""
from datetime import date
from decimal import Decimal
from flask import abort,render_template
from sqlalchemy import text

CATALOG={
 'readings':[('history','Reading History'),('municipal_submission','Municipal Reading Submission')],
 'reconciliation':[('metro_reconciliation','Metro Reconciliation'),('utilities_reconciliation','Utilities Reconciliation')],
 'metsoa':[('metsoa_statement','METSOA Statement')],
 'accounts':[('account_register','Account Register')],
 'arrangements':[('arrangement_register','Arrangement Register')],
}
SOURCES={'readings':'Meter Readings','reconciliation':'Reconciliation','metsoa':'METSOA','accounts':'Municipal Accounts','arrangements':'Payment Arrangements'}
PERIOD_REPORTS={'metsoa_statement','utilities_reconciliation'}


def populate(data,args,session):
    """Reuse original builders; no tariff, consumption or payment arithmetic here."""
    key=data['report'];p=data['property']['id']
    from .municipal_metsoa import month_context,recipient_choices
    from werkzeug.exceptions import HTTPException
    try:data['recipients']=recipient_choices(p,data['property']['manager_id'])
    except HTTPException as exc:
        if exc.code!=503:raise
        data['recipients']=[]
    data['recipient_id']=str(args.get('recipient_id') or '')
    data['test_report']='1' if args.get('test_report')=='1' else ''
    data['report_markup']='';data['report_payload']=None
    data['billing_month']=str(args.get('billing_month') or '')
    if key in PERIOD_REPORTS or key in ('municipal_submission','arrangement_register'):
        period=data.get('by_month') or data['billing_month'] or (data['date_to'][:7] if data['date_to'] else None)
        data['billing_month'],start,end=month_context(period)
    if key in PERIOD_REPORTS:
        data.update(by_month=data['billing_month'],date_from=start.isoformat(),date_to=end.isoformat())
    if key=='municipal_submission':data.update(date_from=data['date_from'] or start.isoformat(),date_to=data['date_to'] or end.isoformat())
    if key=='account_register':data.update(date_from='',date_to='')
    wanted=args.get('preview')=='1' or args.get('format')=='pdf' or args.get('action')=='email'
    if key=='history' or key=='metro_reconciliation' or not wanted:return
    if key=='municipal_submission':
        from .municipal_readings_pdf import report
        first=date.fromisoformat(data['date_from']) if data['date_from'] else start
        last=date.fromisoformat(data['date_to']) if data['date_to'] else end
        groups={'electricity':[],'water':[]};cursor=first.replace(day=1)
        while cursor<=last:
            original=report(p,cursor.strftime('%Y-%m'))
            for side,rows in original['groups'].items():
                groups[side].extend(r for r in rows if first<=r['reading_date']<=last and (not data['item_id'] or str(r['meter_id'])==data['item_id']))
            cursor=date(cursor.year+cursor.month//12,cursor.month%12+1,1)
        payload=dict(month=first.strftime('%Y-%m') if first.replace(day=1)==last.replace(day=1) else first.isoformat()+' to '+last.isoformat(),groups=groups)
        data['report_payload']=payload
        data['report_markup']=render_template('program_billing/output_municipal_submission.html',**payload)
    elif key in PERIOD_REPORTS:
        from .municipal_metsoa import build
        if not data['recipient_id']:abort(400,description='Select a reporting month and registered recipient.')
        statement=build(p,data['billing_month'],recipient_id=data['recipient_id'],test_mode=data['test_report']=='1')
        if data['item_id']:
            account=session.execute(text('SELECT * FROM bil_muni_account WHERE property_id=:p AND id=:id'),dict(p=p,id=int(data['item_id']))).mappings().one()
            meters=session.execute(text('SELECT id,meter_number FROM bil_meter WHERE property_id=:p AND (id=:w OR id=:e OR municipal_bill_number=:n)'),dict(p=p,w=account['water_meter_id'],e=account['elec_meter_id'],n=account['account_number'])).mappings().all()
            numbers={m['meter_number'] for m in meters};ids={m['id'] for m in meters}
            statement['electricity']['rows']=[r for r in statement['electricity']['rows'] if r['meter_number'] in numbers]
            statement['electricity']['subtotal']=sum((r['due'] for r in statement['electricity']['rows']),Decimal(0))
            statement['water']['meters']=[r for r in statement['water']['meters'] if r['meter_number'] in numbers]
            statement['water']['total']=sum((r['total'] for r in statement['water']['meters']),Decimal(0))
            statement['grand_total']=statement['electricity']['subtotal']+statement['water']['total']
            statement['pm_consumption_rows']=[r for r in statement['pm_consumption_rows'] if r['meter_id'] in ids]
            statement['pm_comparisons']=[r for r in statement['pm_comparisons'] if r['account']==account['account_number']]
        data['report_payload']=statement
        template='program_billing/metsoa_compact.html' if key=='metsoa_statement' else 'program_billing/metsoa_comparison.html'
        data['report_markup']=render_template(template,pm_output_report=True,**statement)
    elif key=='account_register':
        from .municipal_accounts import account_rows,meters_for
        rows=[]
        for account in account_rows(p):
            if data['item_id'] and str(account['id'])!=data['item_id']:continue
            meters,issues=meters_for(account)
            rows.append(dict(account,issues=issues,water_number=meters['water']['meter_number'] if meters['water'] else account.get('muni_water_meter_no'),electricity_number=meters['electricity']['meter_number'] if meters['electricity'] else account.get('muni_elec_meter_no')))
        data['report_payload']=rows
        data['report_markup']=render_template('program_billing/output_registers.html',register='accounts',register_rows=rows)
    elif key=='arrangement_register':
        from .municipal_arrangements import register_rows,ready
        if not ready():abort(503,description='Payment arrangement records are unavailable.')
        rows=register_rows(p,data['billing_month'],int(data['item_id']) if data['item_id'] else None)
        first=date.fromisoformat(data['date_from']) if data['date_from'] else None
        last=date.fromisoformat(data['date_to']) if data['date_to'] else None
        rows=[r for r in rows if (not first or r['agreement_date']>=first) and (not last or r['agreement_date']<=last)]
        data['report_payload']=rows
        data['report_markup']=render_template('program_billing/output_registers.html',register='arrangements',register_rows=rows,billing_month=data['billing_month'])
