"""Read-only property Utilities reports, with explicit previewed PDF email."""
import hashlib,json
from datetime import datetime
from zoneinfo import ZoneInfo
from io import BytesIO
from flask import request,render_template,abort,current_app,flash,redirect,url_for,send_file
from flask_login import current_user
from sqlalchemy import text
from itsdangerous import URLSafeTimedSerializer,BadSignature,SignatureExpired
from werkzeug.exceptions import HTTPException
from app.extensions import db
from .metsoa_setup_safety import property_scope

from .utilities_output_reports import CATALOG,SOURCES,PERIOD_REPORTS

FILTERS=('source','report','item_id','date_from','date_to','billing_month','recipient_id','test_report')

def meter_histories(choices,rows,history):
    """Use canonical saved-reading monthly consumption; never derived estimates."""
    from .reading_consumption import derived
    from decimal import Decimal
    grouped=[]
    for meter in choices:
        selected=sorted((r for r in rows if r['meter_id']==meter['id']),key=lambda r:(r['reading_date'],r['id']))
        if not selected:continue
        saved=[r for r in history if r['meter_id']==meter['id']]
        periods={}
        for row in selected:
            period=row['reading_date'].strftime('%Y-%m')
            if period not in periods:periods[period]=derived(meter,period,saved)
        closing={}
        for period,values in periods.items():
            if values:
                matches=[r for r in saved if r['reading_date']==values['new_date'] and Decimal(str(r['reading_value']))==values['new_read']]
                closing[period]=max(matches,key=lambda r:r['id'])['id']
        display=[];consumptions=[]
        for row in selected:
            period=row['reading_date'].strftime('%Y-%m');values=periods[period]
            canonical=bool(values and closing.get(period)==row['id'])
            prior=max((r for r in saved if (r['reading_date'],r['id'])<(row['reading_date'],row['id'])),key=lambda r:(r['reading_date'],r['id']),default=None)
            display.append(dict(row,past_reading=values['last_read'] if canonical else prior['reading_value'] if prior else None,consumption=values['consumption'] if canonical else None))
            if canonical:consumptions.append(values['consumption'])
        total=sum(consumptions,Decimal(0));count=len(consumptions)
        grouped.append(dict(meter=meter,unit='kL' if meter['utility_type'].lower()=='water' else 'kWh',rows=display,valid_months=count,total_consumption=total,average=total/count if count else None))
    return grouped


def report(p,args):
    property_scope(p)
    from .utilities_capture import meters,readings,statements,day
    source=args.get('source','readings')
    if source not in CATALOG:abort(400,description='Select a supported report source.')
    key=args.get('report') or CATALOG[source][0][0]
    if key not in dict(CATALOG[source]):abort(400,description='Select a report belonging to the selected source.')
    account_choices=[dict(r) for r in db.session.execute(text('SELECT id,account_number FROM bil_muni_account WHERE property_id=:p ORDER BY account_number,id'),dict(p=p)).mappings()]
    all_choices=dict(readings=meters(p),**{name:account_choices for name in SOURCES if name!='readings'})
    choices=all_choices[source];item=args.get('item_id','')
    selected=next((m for m in choices if str(m['id'])==item),None) if item else None
    if item and not selected:abort(403)
    date_from=args.get('date_from','');date_to=args.get('date_to','')
    first=day(date_from) if date_from else None;last=day(date_to) if date_to else None
    if first and last and first>last:abort(400,description='Date From must not be later than Date To.')
    history=readings(p) if key=='history' else statements(p) if key=='metro_reconciliation' else []
    rows=[]
    for row in history:
        if item and str(row['meter_id'] if source=='readings' else row['account_id'])!=item:continue
        recorded=row['reading_date'] if source=='readings' else row['statement_date']
        if (first and recorded<first) or (last and recorded>last):continue
        rows.append(row)
    prop=db.session.execute(text('SELECT id,name,address,manager_id FROM bil_property WHERE id=:p'),dict(p=p)).mappings().one()
    groups=meter_histories(choices,rows,history) if key=='history' else []
    if key=='history':rows.sort(key=lambda r:(r['reading_date'],r['id']))
    data=dict(report=key,catalog=CATALOG,source_labels=SOURCES,meter_reports=groups,property=prop,source=source,title=dict(CATALOG[source])[key],choices=choices,all_choices=all_choices,item_id=item,item_label=selected['meter_number' if source=='readings' else 'account_number'] if selected else ('All Meters' if source=='readings' else 'All Accounts'),rows=rows,date_from=date_from,date_to=date_to,generated_at=datetime.now(ZoneInfo('Africa/Johannesburg')),billing_month=args.get('billing_month',''))
    from .utilities_output_reports import populate
    populate(data,args,db.session)
    return data


def digest(data):
    values=dict(property=dict(data['property']),rows=data['rows'],item_label=data['item_label'])
    if data['report']=='history':values['meter_reports']=data['meter_reports']
    if data['report_payload'] is not None:values['report_payload']=data['report_payload']
    return hashlib.sha256(json.dumps(values,sort_keys=True,default=str).encode()).hexdigest()

def serializer():return URLSafeTimedSerializer(current_app.secret_key,salt='pm-utilities-output')

def preview_token(data):
    return serializer().dumps(dict(user=current_user.id,property=data['property']['id'],filters={k:data[k] for k in FILTERS},digest=digest(data),generated_at=data['generated_at'].isoformat()))

def verify_preview(data,token):
    try:saved=serializer().loads(token or '',max_age=1800)
    except (BadSignature,SignatureExpired):abort(400,description='Preview this report again before printing a PDF or emailing it.')
    if saved.get('user')!=current_user.id or saved.get('property')!=data['property']['id'] or saved.get('filters')!={k:data[k] for k in FILTERS}:abort(400,description='Report selection changed. Preview it again before emailing.')
    if saved.get('digest')!=digest(data):abort(409,description='Report records changed after preview. Preview the latest records before emailing.')
    data['generated_at']=datetime.fromisoformat(saved['generated_at'])

def render_pdf(data):
    if data['report']=='municipal_submission':
        from .municipal_readings_pdf import render_pdf as municipal_pdf
        return municipal_pdf(data['report_payload'])
    if data['report']=='metsoa_statement':
        from .municipal_metsoa import pdf_bytes
        return pdf_bytes(data['report_markup'],test_mode=data['test_report']=='1')
    from app.utils.pdf_render import html_to_pdf_bytes
    return html_to_pdf_bytes(render_template('program_billing/utilities_report_pdf.html',**data))

def email_pdf(address,data,pdf):
    from app.utils.mailer import send_pdf_email
    transport = current_app.extensions.get('mail')
    if current_app.config.get('MAIL_SUPPRESS_SEND') or getattr(transport, 'suppress', False):
        raise RuntimeError('Mail transport is suppressed; no SMTP acceptance occurred')
    return send_pdf_email(address,data['property']['name']+' — '+data['title'],'Please find the requested Utilities report attached.',pdf,'Utilities-'+data['report']+'.pdf')

def page(p):
    property_scope(p);args=request.form if request.method=='POST' else request.args
    error=None;status=200
    try:data=report(p,args)
    except HTTPException as exc:
        if exc.code!=400:raise
        fallback=args.to_dict();fallback.pop('date_from',None);fallback.pop('date_to',None)
        if fallback.get('source') not in CATALOG:fallback['source']='readings'
        if fallback.get('report') not in dict(CATALOG[fallback['source']]):fallback['report']=CATALOG[fallback['source']][0][0]
        fallback.pop('preview',None);fallback.pop('format',None);fallback.pop('action',None)
        data=report(p,fallback);data.update(date_from=args.get('date_from',''),date_to=args.get('date_to',''),rows=[],meter_reports=[])
        error=exc.description;status=400
    previewed=not error and args.get('preview')=='1'
    address=args.get('email_to',getattr(current_user,'email','') or '')
    if request.method=='POST' or args.get('format')=='pdf':
        if not error:
            try:
                verify_preview(data,args.get('preview_token'))
                if request.method=='POST':
                    if args.get('action')!='email':abort(400,description='Select Email to send this report.')
                    from email_validator import validate_email,EmailNotValidError
                    try:address=validate_email(address,check_deliverability=False).normalized
                    except EmailNotValidError:abort(400,description='Enter a valid recipient email address.')
                try:pdf=render_pdf(data)
                except Exception:
                    current_app.logger.exception('Utilities report PDF failed');abort(503,description='The PDF could not be generated. Please contact the administrator.')
                if request.method=='GET':return send_file(BytesIO(pdf),mimetype='application/pdf',as_attachment=args.get('download')=='1',download_name='Utilities-'+data['report']+'.pdf')
                try:
                    if not email_pdf(address,data,pdf):raise RuntimeError('Email delivery not confirmed')
                except Exception:
                    current_app.logger.exception('Utilities report email failed');abort(503,description='The report could not be emailed. Delivery was not confirmed; please try again or contact the administrator.')
                flash('Mail transport accepted the Utilities report. Inbox delivery is not confirmed.','success')
                return redirect(url_for('billing_bp.utilities_output',property_id=p,preview='1',**{k:data[k] for k in FILTERS}))
            except HTTPException as exc:error=exc.description;status=exc.code
    return render_template('program_billing/utilities_output.html',**data,previewed=previewed,preview_token=preview_token(data) if previewed else '',email_to=address,error=error),status
