"""Read-only ordinary municipal statement from canonical capture records."""
import calendar,re
from datetime import date,datetime
from decimal import Decimal
from zoneinfo import ZoneInfo
from flask import abort,request,render_template,send_file
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
    entry=dict(number=meter['meter_number'],previous=None,current=None,difference=None,days=None,error=None)
    readings=db.session.execute(text('SELECT id,reading_date,reading_value FROM bil_meter_reading WHERE meter_id=:m AND reading_date<=:end ORDER BY reading_date DESC,id DESC'),dict(m=meter['id'],end=end)).mappings().all()
    by_date={}
    for reading in readings:by_date.setdefault(reading['reading_date'],[]).append(reading)
    dates=sorted(by_date,reverse=True)
    if not dates:entry['error']='No readings are available.'
    elif len(dates)<2:entry['error']='Only one reading is available; consumption cannot yet be calculated.'
    elif dates[0]<start:entry['error']='No reading captured in '+start.strftime('%Y-%m')+'.'
    else:
        pair=[by_date[d] for d in dates[:2]]
        if any(len({Decimal(str(r['reading_value'])) for r in group})!=1 for group in pair):entry['error']='Conflicting readings exist for the same date.'
        else:
            current=pair[0][0];previous=pair[1][0];new=Decimal(str(current['reading_value']));old=Decimal(str(previous['reading_value']))
            if not new.is_finite() or not old.is_finite() or old<0 or new<old:entry['error']='Readings decrease or contain invalid values; review the original records.'
            else:entry.update(previous=dict(date=previous['reading_date'],value=old),current=dict(date=current['reading_date'],value=new),difference=new-old,days=(current['reading_date']-previous['reading_date']).days)
    return entry

def statement_recipient(p,manager_id,month,recipient_id=None):
    from .lease_soa import tenants_for
    from werkzeug.exceptions import HTTPException
    try:choices=tenants_for(p,manager_id=manager_id)
    except HTTPException as exc:
        if exc.code!=503:raise
        return None,[],'Recipient unavailable: registered tenant ownership cannot be verified.'
    if recipient_id not in (None,''):
        try:identifier=int(recipient_id)
        except (ValueError,TypeError):abort(400,description='Select a valid registered tenant recipient.')
        selected=next((t for t in choices if t['id']==identifier),None)
        if not selected:abort(403)
        return selected,choices,None
    if not choices:return None,[],'No registered tenant association is available for this property. Confirm the tenant in the existing property/lease records.'
    if len(choices)>1:return None,choices,'Multiple tenants are linked to this property. Select the METSOA recipient; no tenant was chosen automatically.'
    if month<month_context()[0]:return None,choices,'Select the recipient for this historical billing month; a property association alone does not establish occupancy for that period.'
    candidate=choices[0]
    if candidate.get('draft_id'):
        latest=db.session.execute(text("SELECT id FROM bil_lease_draft WHERE property_id=:p AND manager_id=:u AND status='draft' ORDER BY updated_at DESC,id DESC LIMIT 1"),dict(p=p,u=manager_id)).scalar()
        direct=db.session.execute(text('SELECT 1 FROM bil_tenant WHERE id=:t AND property_id=:p AND manager_id=:u'),dict(t=candidate['id'],p=p,u=manager_id)).first()
        if str(latest)!=candidate['draft_id'] and not direct:return None,choices,'Only a historical draft links this tenant. Select the verified recipient before finalising the statement.'
    return candidate,choices,None

def build(p,month,recipient_id=None):
    property_scope(p);month,start,end=month_context(month)
    prop=db.session.execute(text('SELECT id,name,address,manager_id FROM bil_property WHERE id=:p'),dict(p=p)).mappings().one()
    accounts=[dict(r) for r in db.session.execute(text('SELECT id,account_number,water_meter_id,elec_meter_id FROM bil_muni_account WHERE property_id=:p'),dict(p=p)).mappings()]
    meters=[dict(m) for m in db.session.execute(text('SELECT * FROM bil_meter WHERE property_id=:p ORDER BY meter_number,id'),dict(p=p)).mappings()]
    from .metsoa_tariffs import select
    tariffs,tariff_issues=select(end)
    issues=list(tariff_issues);source=[]
    if not meters:issues.append('No registered property meters are available.')
    for meter in meters:
        linked=[a for a in accounts if meter['id'] in (a['water_meter_id'],a['elec_meter_id']) or meter.get('municipal_bill_number')==a['account_number']]
        if len(linked)!=1:
            issues.append(meter['meter_number']+': municipal account association is missing or ambiguous.');continue
        if meter.get('municipal_bill_number') not in (None,'',linked[0]['account_number']):
            issues.append(meter['meter_number']+': meter pointer conflicts with its municipal account number.');continue
        kind=(meter.get('utility_type') or '').lower()
        if kind not in ('water','electricity','electrical'):
            issues.append(meter['meter_number']+': unsupported utility type.');continue
        pair=reading_pair(meter,start,end)
        if pair['error']:issues.append(meter['meter_number']+': '+pair['error']);continue
        if kind=='water':
            if not tariffs['water'] or not tariffs['sewerage']:continue
            if any(sum((t['multiplier']*pair['days'] for t in tariffs[side]),Decimal(0))<pair['difference'] for side in ('water','sewerage')):
                issues.append(meter['meter_number']+': tariff bands do not cover the recorded consumption; no excess rate was guessed.');continue
        elif tariffs['electricity'] is None:continue
        if any(pair['previous']['date']<r.effective_date<=pair['current']['date'] for r in tariffs['records']):
            issues.append(meter['meter_number']+': tariff effective date changes within its reading period; review rate allocation before finalisation.');continue
        source.append(SimpleNamespace(meter_id=meter['id'],meter_number=meter['meter_number'],utility_type=meter['utility_type'],last_date=pair['previous']['date'],new_date=pair['current']['date'],last_read=pair['previous']['value'],new_read=pair['current']['value'],days=pair['days'],consumption=pair['difference']))
    elec_builder,water_builder=original_builders()
    electricity,electricity_total=elec_builder(p,month,source=source,tariffs=tariffs)
    water,water_total=water_builder(p,month,source=source,tariffs=tariffs)
    recipient,recipient_choices,recipient_issue=statement_recipient(p,prop['manager_id'],month,recipient_id)
    if recipient_issue:issues.append(recipient_issue)
    return dict(property=prop,month=month,tenant=recipient,electricity=dict(rows=electricity,subtotal=electricity_total),water=dict(meters=water,total=water_total),grand_total=electricity_total+water_total,pm_tariff_statement=True,pm_complete=not issues,pm_issues=issues,pm_customer=recipient['name'] if recipient else '',pm_recipient=recipient,pm_recipients=recipient_choices,pm_recipient_issue=recipient_issue,pm_electricity_available=bool(electricity),pm_statement_date=end,pm_tariff_records=tariffs['records'])

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
    property_scope(p);month,_,_=month_context(request.args.get('billing_month'))
    # Existing PM token-free access remains unchanged.
    data=build(p,month,recipient_id=request.args.get('recipient_id'))
    html=render_template('program_billing/metsoa.html',**data)
    if request.args.get('format')=='pdf':
        return send_file(BytesIO(pdf_bytes(html)),mimetype='application/pdf',as_attachment=False,download_name=month+'-MetSoa-'+data['property']['name']+'.pdf')
    return html
