"""Read-only ordinary municipal statement from canonical capture records."""
import calendar,re
from datetime import date,datetime
from decimal import Decimal
from zoneinfo import ZoneInfo
from flask import abort,request,render_template
from sqlalchemy import text
from app.extensions import db
from .metsoa_setup_safety import property_scope
from .utilities_capture import available,paid

def month_context(value=None):
    value=value or datetime.now(ZoneInfo('Africa/Johannesburg')).strftime('%Y-%m')
    if not re.fullmatch(r'\d{4}-\d{2}',value):abort(400)
    try:
        year,month=map(int,value.split('-'));start=date(year,month,1);end=date(year,month,calendar.monthrange(year,month)[1])
    except ValueError:abort(400)
    return value,start,end

def build(p,month):
    property_scope(p);month,start,end=month_context(month)
    prop=db.session.execute(text('SELECT id,name,address,manager_id FROM bil_property WHERE id=:p'),dict(p=p)).mappings().one()
    accounts=[dict(a) for a in db.session.execute(text('SELECT * FROM bil_muni_account WHERE property_id=:p ORDER BY account_number,id'),dict(p=p)).mappings()]
    meters=[dict(m) for m in db.session.execute(text('SELECT * FROM bil_meter WHERE property_id=:p ORDER BY meter_number,id'),dict(p=p)).mappings()]
    warnings=[];result=[];used=set();totals={k:Decimal(0) for k in ('arrears','balance','due','paid')};bill_complete=bool(accounts);paid_complete=True
    has_bills=available('bil_municipal_statement',['account_id','period','statement_date','arrears','balance','due','due_date'])
    for account in accounts:
        issues=[];rows=[]
        linked=[m for m in meters if m['id'] in (account.get('water_meter_id'),account.get('elec_meter_id')) or m.get('municipal_bill_number')==account['account_number']]
        if not linked:issues.append('No meters are linked to this municipal account.')
        for meter in linked:
            used.add(meter['id']);entry=dict(number=meter['meter_number'],unit='kL' if (meter.get('utility_type') or '').lower()=='water' else 'kWh',previous=None,current=None,difference=None,days=None,error=None)
            shared=any(a['id']!=account['id'] and (meter['id'] in (a.get('water_meter_id'),a.get('elec_meter_id')) or meter.get('municipal_bill_number')==a['account_number']) for a in accounts)
            if shared or meter.get('municipal_bill_number') not in (None,'',account['account_number']):entry['error']='Conflicting account links require review.'
            elif (meter.get('utility_type') or '').lower() not in ('water','electricity','electrical'):entry['error']='Unsupported utility type; no consumption calculated.'
            else:
                readings=db.session.execute(text('SELECT id,reading_date,reading_value FROM bil_meter_reading WHERE meter_id=:m AND reading_date<=:end ORDER BY reading_date DESC,id DESC'),dict(m=meter['id'],end=end)).mappings().all()
                by_date={}
                for reading in readings:by_date.setdefault(reading['reading_date'],[]).append(reading)
                dates=sorted(by_date,reverse=True)
                if not dates:entry['error']='No readings are available.'
                elif len(dates)<2:entry['error']='Only one reading is available; consumption cannot yet be calculated.'
                elif dates[0]<start:entry['error']='No reading captured in '+month+'.'
                else:
                    pair=[by_date[d] for d in dates[:2]]
                    if any(len({Decimal(str(r['reading_value'])) for r in group})!=1 for group in pair):entry['error']='Conflicting readings exist for the same date.'
                    else:
                        current=pair[0][0];previous=pair[1][0];new=Decimal(str(current['reading_value']));old=Decimal(str(previous['reading_value']))
                        if not new.is_finite() or not old.is_finite() or old<0 or new<old:entry['error']='Readings decrease or contain invalid values; review the original records.'
                        else:entry.update(previous=dict(date=previous['reading_date'],value=old),current=dict(date=current['reading_date'],value=new),difference=new-old,days=(current['reading_date']-previous['reading_date']).days)
            if entry['error']:issues.append(meter['meter_number']+': '+entry['error'])
            rows.append(entry)
        bill=db.session.execute(text('SELECT * FROM bil_municipal_statement WHERE account_id=:a AND period=:m'),dict(a=account['id'],m=month)).mappings().first() if has_bills else None
        if bill:
            for key in ('arrears','balance','due'):totals[key]+=bill[key]
        else:
            bill_complete=False;issues.append('Capture the '+month+' municipal bill: statement date, arrears, balance, due and due date in Metro Reconciliation.')
        payment=paid(account['id'],month)
        if payment is None:paid_complete=False;issues.append('Paid is unavailable: municipal-account/month payment allocation is not available.')
        else:totals['paid']+=payment
        result.append(dict(account=account,readings=rows,bill=bill,paid=payment,issues=issues))
    if not accounts:warnings.append('No municipal accounts are registered against this property.')
    for meter in meters:
        if meter['id'] not in used:warnings.append(meter['meter_number']+' has no verified municipal-account link. Review Municipal Accounts.')
    return dict(property=prop,month=month,accounts=result,warnings=warnings,totals=totals,bill_complete=bill_complete,paid_complete=paid_complete,complete=bool(accounts) and not warnings and all(not a['issues'] for a in result))

def page(p):
    property_scope(p);month,_,_=month_context(request.args.get('billing_month'))
    # PM managers may view their property statements without a token purchase.
    return render_template('program_billing/municipal_metsoa.html',**build(p,month))
