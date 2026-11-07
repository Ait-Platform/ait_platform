"""Read-only ordinary municipal statement from canonical capture records."""
import calendar,re
from datetime import date,datetime
from decimal import Decimal
from zoneinfo import ZoneInfo
from flask import abort,request,render_template,send_file,current_app
from sqlalchemy import text
from app.extensions import db
from .metsoa_setup_safety import property_scope
from types import SimpleNamespace
from io import BytesIO

def month_context(value=None):
    value=value or datetime.now(ZoneInfo('Africa/Johannesburg')).strftime('%Y-%m')
    if not re.fullmatch(r'\d{4}-\d{2}',value):abort(400)
    try:
        year,month=map(int,value.split('-'));start=date(year,month,1);end=date(year,month,calendar.monthrange(year,month)[1])
    except ValueError:abort(400)
    return value,start,end

def original_builders():
    from .routes import build_electrical_rows,build_water_rows
    return build_electrical_rows,build_water_rows

def reading_pair(meter,start,end):
    readings=db.session.execute(text('SELECT id,reading_date,reading_value FROM bil_meter_reading WHERE meter_id=:m AND reading_date<=:end ORDER BY reading_date DESC,id DESC'),dict(m=meter['id'],end=end)).mappings().all()
    return reading_pair_from_rows(meter,start,end,readings)


def reading_pair_from_rows(meter,start,end,readings):
    entry=dict(number=meter['meter_number'],billing_month=None,previous=None,current=None,difference=None,days=None,error=None)
    previous_start=date(start.year-(start.month==1),12 if start.month==1 else start.month-1,1)
    current_rows=[r for r in readings if start<=r['reading_date']<=end]
    previous_rows=[r for r in readings if previous_start<=r['reading_date']<start]
    if not current_rows:entry['error']='No reading captured in '+start.strftime('%Y-%m')+'.'
    elif not previous_rows:entry['error']='No reading captured in previous month '+previous_start.strftime('%Y-%m')+'; consumption cannot yet be calculated.'
    elif any(not Decimal(str(r['reading_value'])).is_finite() or Decimal(str(r['reading_value']))<0 for r in current_rows+previous_rows):entry['error']='Readings decrease or contain invalid values; review the original records.'
    else:
        current=max(current_rows,key=lambda r:(Decimal(str(r['reading_value'])),r['reading_date'],r['id']))
        previous=max(previous_rows,key=lambda r:(Decimal(str(r['reading_value'])),r['reading_date'],r['id']))
        new=Decimal(str(current['reading_value']));old=Decimal(str(previous['reading_value']))
        if new<old:entry['error']='Readings decrease or contain invalid values; review the original records and existing meter reset/replacement rules.'
        else:entry.update(billing_month=start.strftime('%Y-%m'),previous=dict(date=previous['reading_date'],value=old),current=dict(date=current['reading_date'],value=new),difference=new-old,days=(current['reading_date']-previous['reading_date']).days)
    return entry

def recipient_choices(p,manager_id):
    from .lease_soa import tenants_for
    from .municipal_accounts import registered_owners
    from werkzeug.exceptions import HTTPException
    choices=[]
    try:choices=[dict(t,role='Tenant') for t in tenants_for(p,manager_id=manager_id)]
    except HTTPException as exc:
        if exc.code!=503:raise
    choices.extend(dict(o,id='owner:'+str(o['id']),role='Owner',email=None) for o in registered_owners(p))
    return choices

def statement_recipient(p,manager_id,month,recipient_id=None):
    choices=recipient_choices(p,manager_id)
    if recipient_id not in (None,''):
        selected=next((c for c in choices if str(c['id'])==str(recipient_id)),None)
        if not selected:abort(403,description='Select a registered owner or tenant linked to this property.')
        return selected,choices,None
    return None,choices,'Select a registered property owner or tenant as the METSOA recipient.'

def bulk_consumption(meter,meters,pairs,issues,notes):
    """Subtract validated direct-child usage assigned to the same billing month."""
    from .bulk_meter_linking import kind
    pair=pairs[meter['id']]
    children=[m for m in meters if m.get('parent_meter_id')==meter['id']]
    if not children:return pair['difference']
    by_id={m['id']:m for m in meters}
    for child in children:
        visited=set();node=child['id']
        while node in by_id:
            if node in visited:
                issues.append(meter['meter_number']+': circular bulk relationships require correction.');return None
            visited.add(node);node=by_id[node].get('parent_meter_id')
        if kind(child['utility_type'])!=kind(meter['utility_type']):
            issues.append(meter['meter_number']+': linked meters have different utility types.');return None
        cp=pairs[child['id']]
        if cp['error'] or not cp['billing_month'] or cp['billing_month']!=pair['billing_month']:
            reason=cp['error'] or 'Validated consumption is not available for the selected billing month.'
            issues.append(meter['meter_number']+': linked meter '+child['meter_number']+' monthly consumption for '+str(pair['billing_month'])+' cannot be established: '+reason+' Bulk adjustment is incomplete.');return None
        if (cp['previous']['date'],cp['current']['date']) != (pair['previous']['date'],pair['current']['date']):
            issues.append(meter['meter_number']+': linked meter '+child['meter_number']+' has incompatible reading intervals ('+str(cp['previous']['date'])+' to '+str(cp['current']['date'])+'; bulk '+str(pair['previous']['date'])+' to '+str(pair['current']['date'])+'). No bulk adjustment was calculated.')
            return None
    deducted=sum((pairs[c['id']]['difference'] for c in children),Decimal(0))
    adjusted=pair['difference']-deducted
    message=meter['meter_number']+': bulk consumption '+str(pair['difference'])+' minus linked individual consumption '+str(deducted)+' = '+str(adjusted)+(' kL' if kind(meter['utility_type'])=='water' else ' kWh')+'.'
    if adjusted<0:message+=' Negative difference shown for investigation; no automatic refund is created.'
    notes.append(message)
    return adjusted

def applicable_tariff_records(utility,records):
    """Only charge components used by this utility may constrain its interval."""
    water = utility == 'water'
    for record in records:
        kind = (record.utility_type or '').strip().lower()
        code = record.code.strip().lower()
        if water:
            if getattr(record,'charge_component',None) in ('water_loss','water_surcharge','sewerage_surcharge') or kind in ('water','sewerage','sanitation','water_fixed','sewerage_fixed','sanitation_fixed') or code in ('w_loss','w_mgt','s_refuse','w_surcharge','s_surcharge'):
                yield record
        elif kind in ('electricity','electrical'):
            yield record


def build(p,month,recipient_id=None,use_current_tariffs=False):
    property_scope(p);month,start,end=month_context(month)
    prop=db.session.execute(text('SELECT id,name,address,manager_id FROM bil_property WHERE id=:p'),dict(p=p)).mappings().one()
    accounts=[dict(r) for r in db.session.execute(text('SELECT id,account_number,water_meter_id,elec_meter_id FROM bil_muni_account WHERE property_id=:p'),dict(p=p)).mappings()]
    meters=[dict(m) for m in db.session.execute(text('SELECT * FROM bil_meter WHERE property_id=:p ORDER BY meter_number,id'),dict(p=p)).mappings()]
    from .bulk_meter_linking import confirmed_meters
    meters=confirmed_meters(p,meters)
    from .metsoa_tariffs import select
    tariffs,tariff_issues=select(end,use_current=use_current_tariffs)
    issues=list(tariff_issues);source=[];bulk_notes=[];consumption_rows=[]
    pairs={m['id']:reading_pair(m,start,end) for m in meters}
    if not meters:issues.append('No registered property meters are available.')
    for meter in meters:
        pair=pairs[meter['id']]
        row=dict(meter_id=meter['id'],number=meter['meter_number'],utility=meter['utility_type'],previous=pair['previous'],current=pair['current'],consumption=pair['difference'],bulk_difference=None,is_bulk=any(m.get('parent_meter_id')==meter['id'] for m in meters),error=pair['error'])
        consumption_rows.append(row)
        measured=bulk_consumption(meter,meters,pairs,issues,bulk_notes) if not pair['error'] else None
        if row['is_bulk']:row['bulk_difference']=measured
        linked=[a for a in accounts if meter['id'] in (a['water_meter_id'],a['elec_meter_id']) or meter.get('municipal_bill_number')==a['account_number']]
        if len(linked)!=1:
            issues.append(meter['meter_number']+': municipal account association is missing or ambiguous.');continue
        if meter.get('municipal_bill_number') not in (None,'',linked[0]['account_number']):
            issues.append(meter['meter_number']+': meter pointer conflicts with its municipal account number.');continue
        kind=(meter.get('utility_type') or '').lower()
        if kind not in ('water','electricity','electrical'):
            issues.append(meter['meter_number']+': unsupported utility type.');continue
        pair=pairs[meter['id']]
        if pair['error']:issues.append(meter['meter_number']+': '+pair['error']);continue
        consumption=measured
        if consumption is None:continue
        # Display the signed measured difference; pricing never creates a refund.
        consumption=max(Decimal(0),consumption)
        if kind=='water':
            if not tariffs['water'] or not tariffs['sewerage']:continue
            if any(sum((t['multiplier']*pair['days'] for t in tariffs[side]),Decimal(0))<consumption for side in ('water','sewerage')):
                issues.append(meter['meter_number']+': tariff bands do not cover the recorded consumption; no excess rate was guessed.');continue
        elif tariffs['electricity'] is None:continue
        if any(pair['previous']['date']<r.effective_date<=pair['current']['date'] for r in applicable_tariff_records(kind,tariffs['records'])):
            issues.append(meter['meter_number']+': tariff effective date changes within its reading period; review rate allocation before finalisation.');continue
        source.append(SimpleNamespace(estimated=any(getattr(r,'historical_estimate',False) for r in applicable_tariff_records(kind,tariffs['records'])),account_id=linked[0]['id'],meter_id=meter['id'],meter_number=meter['meter_number'],utility_type=meter['utility_type'],last_date=pair['previous']['date'],new_date=pair['current']['date'],last_read=pair['previous']['value'],new_read=pair['current']['value'],days=pair['days'],consumption=consumption))
    elec_builder,water_builder=original_builders()
    electricity,electricity_total=elec_builder(p,month,source=source,tariffs=tariffs)
    water,water_total=water_builder(p,month,source=source,tariffs=tariffs)
    priced={r.meter_id for r in source}
    unpriced=[dict(number=r['number'],component=r['utility']+' consumption charges',reason=r['error'] or 'See the missing rate, account or interval warnings.') for r in consumption_rows if r['meter_id'] not in priced]
    missing=list(tariffs.get('unavailable_components',[]))
    for key,label in [('electricity','Electricity consumption'),('water','Water consumption bands'),('sewerage','Sewerage consumption bands'),('water_fixed','Water fixed charges'),('sewerage_fixed','Sewerage fixed charges'),('water_surcharge','Water surcharge'),('sewerage_surcharge','Sewerage surcharge')]:
        if key=='sewerage_surcharge' and not tariffs.get('sewerage_surcharge_applicable',False):continue
        if tariffs[key] is None or tariffs[key]==[]:missing.append(dict(number='All applicable meters',component=label,reason='No applicable verified rate for '+month+'.'))
    if not any(getattr(r,'charge_component',None)=='water_loss' or r.code.lower()=='w_loss' for r in tariffs['water_fixed']):missing.append(dict(number='Water meters',component='Water Loss Levy (insurance)',reason='No applicable W_LOSS rate for '+month+'.'))
    # Compare only complete account calculations with explicit same-account/month bill evidence.
    from .utilities_capture import available
    billed={}
    if available('bil_municipal_statement',['account_id','period','due']):
        for bill in db.session.execute(text('SELECT s.account_id,s.due FROM bil_municipal_statement s JOIN bil_muni_account a ON a.id=s.account_id WHERE a.property_id=:p AND s.period=:month'),dict(p=p,month=month)).mappings():
            billed.setdefault(bill['account_id'],[]).append(bill['due'])
    calculated={a['id']:Decimal(0) for a in accounts}
    for record,line in zip([r for r in source if r.utility_type.lower() in ('electricity','electrical')],electricity):calculated[record.account_id]+=Decimal(str(line['due']))
    for record,line in zip([r for r in source if r.utility_type.lower()=='water'],water):calculated[record.account_id]+=Decimal(str(line['total']))
    comparisons=[]
    for account in accounts:
        attached=[m for m in meters if m['id'] in (account['water_meter_id'],account['elec_meter_id']) or m.get('municipal_bill_number')==account['account_number']]
        complete=bool(attached) and all(m['id'] in priced and not any(i.startswith(m['meter_number']+':') for i in issues) for m in attached)
        for meter in attached:
            utility=(meter['utility_type'] or '').lower()
            if utility=='water':
                complete=complete and all(tariffs[k] for k in ('water','sewerage','water_fixed','sewerage_fixed')) and tariffs['water_surcharge'] is not None and (not tariffs.get('sewerage_surcharge_applicable',False) or tariffs['sewerage_surcharge'] is not None) and any(getattr(r,'charge_component',None)=='water_loss' or r.code.lower()=='w_loss' for r in tariffs['water_fixed'])
            relevant=('water','sewerage','sanitation','water_fixed','sewerage_fixed','sanitation_fixed','water_surcharge','sewerage_surcharge') if utility=='water' else ('electricity','electrical')
            if any((r.get('utility_type') or '').lower() in relevant for r in tariffs.get('unavailable_components',[])):complete=False
        bills=billed.get(account['id'],[])
        bill_amount=None
        if len(bills)==1 and bills[0] is not None:
            try:
                value=Decimal(str(bills[0]))
                if value.is_finite():bill_amount=value.quantize(Decimal('.01'))
            except ArithmeticError:pass
        total=calculated[account['id']].quantize(Decimal('.01')) if complete else None
        difference=total-bill_amount if total is not None and bill_amount is not None else None
        estimated=any(r.estimated for r in source if r.account_id==account['id'])
        comparisons.append(dict(estimated=estimated,account=account['account_number'],calculated=total,billed=bill_amount,difference=difference,label=(('Estimated ' if estimated else '')+('Debit difference' if difference>0 else 'Credit difference' if difference<0 else 'No difference')) if difference is not None else 'Comparison unavailable'))
    recipient,recipient_choices,recipient_issue=statement_recipient(p,prop['manager_id'],month,recipient_id)
    if recipient_issue:issues.append(recipient_issue)
    grouped_components={}
    for component in unpriced+missing:
        key=(component['component'],component['reason'])
        if key not in grouped_components:grouped_components[key]=dict(component)
        elif component['number'] not in grouped_components[key]['number'].split(', '):grouped_components[key]['number']+=', '+component['number']
    estimated_components=[dict(code=r.code,utility=r.utility_type,rate=r.rate,effective_date=r.effective_date) for r in tariffs['records'] if getattr(r,'historical_estimate',False)]
    estimated=bool(estimated_components)
    issues=list(dict.fromkeys(issues))
    report_status=('ESTIMATED (INCOMPLETE)' if issues else 'ESTIMATED') if estimated else ('INCOMPLETE' if issues else 'FINAL')
    display_issues=[i for i in issues if not (i.startswith(('No applicable ','No electricity tariff','No verified effective W_LOSS','Tariff ')) and ('Rate unavailable' in i or 'No applicable ' in i or 'No electricity tariff' in i or 'No verified effective W_LOSS' in i))]
    if missing:display_issues.insert(0,'Rate unavailable for: '+', '.join(dict.fromkeys(r['component'] for r in missing))+'. See the unpriced components below.')
    return dict(property=prop,month=month,tenant=recipient,electricity=dict(rows=electricity,subtotal=electricity_total),water=dict(meters=water,total=water_total),grand_total=electricity_total+water_total,pm_sewerage_surcharge_configured=tariffs.get('sewerage_surcharge_applicable',False),pm_comparisons=comparisons,pm_consumption_rows=consumption_rows,pm_unpriced_components=list(grouped_components.values()),pm_override_requested=use_current_tariffs,pm_estimated=estimated,pm_estimated_components=estimated_components,pm_current_tariff_date=tariffs.get('current_tariff_date'),pm_status=report_status,pm_pricing_complete=not issues,pm_tariff_statement=True,pm_complete=not issues and not estimated,pm_display_issues=display_issues,pm_issues=issues,pm_customer=recipient['name'] if recipient else '',pm_recipient=recipient,pm_recipients=recipient_choices,pm_recipient_issue=recipient_issue,pm_electricity_available=bool(electricity),pm_statement_date=end,pm_tariff_records=tariffs['records'],pm_bulk_notes=bulk_notes)

def pdf_bytes(html):
    from playwright.sync_api import sync_playwright
    with sync_playwright() as runtime:
        browser=runtime.chromium.launch(headless=True)
        try:
            page=browser.new_page()
            page.route('**/*',lambda route:route.abort())
            page.set_content(html,wait_until='domcontentloaded')
            page.add_style_tag(content='body{font-family:Arial,sans-serif;font-size:11px;color:#111}table{width:100%;border-collapse:collapse}th,td{padding:4px;border-bottom:1px solid #ddd}thead{display:table-header-group}tr{break-inside:avoid}.text-right{text-align:right}.text-center{text-align:center}.font-bold,.font-semibold{font-weight:bold}.flex{display:flex}.w-24{width:6rem;display:inline-block;flex-shrink:0}.bg-gray-100,.bg-gray-200{background:#eee}.print-page{padding:12px}.print\\:hidden,nav,script{display:none!important}h1{font-size:20px}h2{font-size:16px}')
            return page.pdf(format='A4',print_background=True,display_header_footer=True,header_template='<span></span>',footer_template='<div style="width:100%;text-align:center;font-size:10px">Page <span class="pageNumber"></span> of <span class="totalPages"></span></div>',margin=dict(top='30px',bottom='40px',left='10px',right='10px'))
        finally:browser.close()

def page(p):
    property_scope(p)
    prop=db.session.execute(text('SELECT id,name,address,manager_id FROM bil_property WHERE id=:p'),dict(p=p)).mappings().one()
    generate=request.args.get('generate')=='1'
    month=request.args.get('billing_month','') if generate else ''
    recipient=request.args.get('recipient_id','') if generate else ''
    use_current=request.args.get('use_current_tariffs')=='1'
    if not generate or not month or not recipient:
        return render_template('program_billing/metsoa_select.html',property=prop,month=month,pm_recipients=recipient_choices(p,prop['manager_id']),pm_recipient=None,pm_override_requested=use_current,error='Select a billing month and recipient.' if generate else None),400 if generate else 200
    month_context(month)
    # Existing PM token-free access remains unchanged.
    data=build(p,month,recipient_id=recipient,use_current_tariffs=use_current)
    if data['pm_estimated']:
        current_app.logger.info('PM historical tariff estimate confirmed actor=%s property=%s month=%s current_schedule_date=%s components=%s',data['property']['manager_id'],p,month,data['pm_current_tariff_date'],','.join(r['code'] for r in data['pm_estimated_components']))
    html=render_template('program_billing/metsoa.html',**data)
    if request.args.get('format')=='pdf':
        return send_file(BytesIO(pdf_bytes(html)),mimetype='application/pdf',as_attachment=request.args.get('download')=='1',download_name=month+'-MetSoa-'+data['property']['name']+'.pdf')
    return html
