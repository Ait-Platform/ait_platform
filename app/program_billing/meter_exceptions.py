"""Account-scoped meter exceptions; legacy meter history remains read-only."""
import json,uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from io import BytesIO
from flask import request,render_template,abort,flash,redirect,url_for,current_app,send_file
from flask_login import current_user
from werkzeug.exceptions import HTTPException
from sqlalchemy import text
from app.extensions import db
from .metsoa_setup_safety import property_scope

def accounts(p):
    return [dict(r) for r in db.session.execute(text('SELECT a.*,o.name AS owner_name FROM bil_muni_account a LEFT JOIN ref_muni_owner o ON o.id=a.owner_id WHERE a.property_id=:p ORDER BY a.account_number,a.id'),dict(p=p)).mappings()]

def account(p,value):
    try:identifier=int(value)
    except (TypeError,ValueError):abort(400,description='Select a municipal account.')
    row=next((a for a in accounts(p) if a['id']==identifier),None)
    if not row:abort(403)
    return row

def account_meters(p,acc):
    rows=[dict(r) for r in db.session.execute(text('SELECT * FROM bil_meter WHERE property_id=:p AND (id=:w OR id=:e OR municipal_bill_number=:n) ORDER BY meter_number,id'),dict(p=p,w=acc['water_meter_id'],e=acc['elec_meter_id'],n=acc['account_number'])).mappings()]
    return rows

TYPES=('Stolen','Missing','Removed','Disconnected','Reconnected','Replaced')

def ready():
    from .utilities_capture import available
    return all(available(t,c) for t,c in [('bil_meter_exception',['id','property_id','account_id','meter_id','status','version','submission_key']),('bil_meter_exception_event',['id','exception_id','kind','details']),('bil_meter_exception_evidence',['id','exception_id','storage_key','sha256'])])

def require_ready():
    if not ready():abort(503,description='Meter exception registration is temporarily unavailable. Please contact the administrator.')

def clean_text(form,key,limit,required=False):
    value=str(form.get(key) or '').strip()
    if len(value)>limit or any(ord(c)<32 and c not in '\n\r\t' for c in value) or (required and not value):abort(400,description='Enter valid '+key.replace('_',' ')+f' (maximum {limit} characters).')
    return value

def identifier(value,label):
    try:return int(value)
    except (ValueError,TypeError):abort(400,description='Select a valid '+label+'.')

def event(e,kind,details):
    db.session.execute(text('INSERT INTO bil_meter_exception_event(exception_id,kind,actor_id,details) VALUES(:e,:k,:u,CAST(:d AS json))'),dict(e=e,k=kind,u=current_user.id,d=json.dumps(details,default=str)))

def scoped(p,e,lock=False):
    property_scope(p);require_ready()
    row=db.session.execute(text('SELECT e.* FROM bil_meter_exception e JOIN bil_muni_account a ON a.id=e.account_id WHERE e.property_id=:p AND a.property_id=:p AND e.id=:e'+(' FOR UPDATE OF e' if lock else '')),dict(p=p,e=identifier(e,'exception'))).mappings().first()
    if not row:abort(404)
    return dict(row)

def checked_meter(p,acc,value):
    mid=identifier(value,'meter');row=next((m for m in account_meters(p,acc) if m['id']==mid),None)
    if not row:abort(400,description='Select a meter registered to this property and municipal account.')
    if row.get('municipal_bill_number') not in (None,'',acc['account_number']):abort(409,description='Meter and account associations conflict. Resolve the historical association before recording this incident.')
    other=db.session.execute(text('SELECT 1 FROM bil_muni_account WHERE id<>:a AND (water_meter_id=:m OR elec_meter_id=:m OR account_number=:n) LIMIT 1'),dict(a=acc['id'],m=mid,n=row.get('municipal_bill_number'))).first()
    if other:abort(409,description='This meter has conflicting municipal account relationships; no incident was saved.')
    return row

def evidence_add(p,record,file):
    if not file or not file.filename:abort(400,description='Select a supporting evidence file.')
    from . import meter_exception_evidence as storage
    try:meta=storage.validate(file)
    except ValueError as exc:abort(400,description=str(exc))
    old=db.session.execute(text('SELECT id FROM bil_meter_exception_evidence WHERE exception_id=:e AND sha256=:h'),dict(e=record['id'],h=meta['sha256'])).scalar()
    if old:return old
    eid=uuid.uuid4();meta['storage_key']=storage.key(p,record['account_id'],record['id'],eid,meta['extension'])
    try:storage.upload(meta,meta['data'])
    except Exception:
        current_app.logger.exception('Private meter evidence upload failed');abort(503,description='Private evidence storage is unavailable. No evidence or incident changes were committed.')
    db.session.execute(text('INSERT INTO bil_meter_exception_evidence(id,exception_id,filename,mime_type,storage_key,size_bytes,sha256,created_by) VALUES(:id,:e,:n,:m,:k,:s,:h,:u)'),dict(id=eid,e=record['id'],n=meta['filename'],m=meta['mime_type'],k=meta['storage_key'],s=meta['size_bytes'],h=meta['sha256'],u=current_user.id))
    event(record['id'],'evidence_added',dict(evidence_id=str(eid),filename=meta['filename'],sha256=meta['sha256']))
    return eid

def create(p,form,file=None):
    property_scope(p,lock=True);require_ready()
    acc=account(p,form.get('account_id'));meter=checked_meter(p,acc,form.get('meter_id'))
    from .utilities_capture import day
    from .bulk_meter_linking import kind
    incident=day(form.get('incident_date'));today=datetime.now(ZoneInfo('Africa/Johannesburg')).date()
    notification=day(form['notification_date']) if form.get('notification_date') else None
    replacement_date=day(form['replacement_date']) if form.get('replacement_date') else None
    if incident>today or any(d and (d<incident or d>today) for d in (notification,replacement_date)):abort(400,description='Incident dates cannot be in the future; notification and replacement dates must not precede the incident.')
    exception_type=form.get('exception_type')
    if exception_type not in TYPES:abort(400,description='Select a supported meter exception type.')
    replacement=checked_meter(p,acc,form['replacement_meter_id']) if form.get('replacement_meter_id') else None
    if bool(replacement)!=bool(replacement_date) or (exception_type=='Replaced' and not replacement):abort(400,description='A replacement requires both an existing replacement meter and its replacement date.')
    if replacement:
        if replacement['id']==meter['id'] or kind(replacement['utility_type'])!=kind(meter['utility_type']):abort(400,description='A replacement must be a different meter of the same utility type.')
        links=[(r[0],r[1]) for r in db.session.execute(text('SELECT meter_id,replacement_meter_id FROM bil_meter_exception WHERE property_id=:p AND replacement_meter_id IS NOT NULL UNION SELECT replacement_for_meter_id,id FROM bil_meter WHERE property_id=:p AND replacement_for_meter_id IS NOT NULL'),dict(p=p))]
        if any(child==replacement['id'] and parent!=meter['id'] for parent,child in links):abort(409,description='The replacement meter already replaces a different meter.')
        graph={}
        for parent,child in links+[(meter['id'],replacement['id'])]:graph.setdefault(parent,set()).add(child)
        def cycle(node,path):
            if node in path:return True
            return any(cycle(child,path|{node}) for child in graph.get(node,()))
        if cycle(meter['id'],set()):abort(400,description='This replacement would create a circular meter relationship.')
    try:key=uuid.UUID(form.get('submission_key',''))
    except (ValueError,TypeError,AttributeError):abort(400,description='Reopen the exception form before saving.')
    values=dict(p=p,a=acc['id'],m=meter['id'],t=exception_type,d=incident,n=notification,ref=clean_text(form,'municipal_reference',150),description=clean_text(form,'description',5000,True),remarks=clean_text(form,'remarks',5000),r=replacement['id'] if replacement else None,rd=replacement_date,key=key,u=current_user.id)
    existing=db.session.execute(text('SELECT * FROM bil_meter_exception WHERE submission_key=:key'),dict(key=key)).mappings().first()
    if existing:
        if existing['property_id']!=p or existing['account_id']!=acc['id'] or existing['created_by']!=current_user.id:abort(409)
        expected=dict(meter_id=values['m'],exception_type=values['t'],incident_date=values['d'],notification_date=values['n'],municipal_reference=values['ref'],description=values['description'],remarks=values['remarks'],replacement_meter_id=values['r'],replacement_date=values['rd'])
        if any(existing[k]!=v for k,v in expected.items()):abort(409,description='This submission already saved a different incident. Reopen the form for a new incident.')
        if file and file.filename:
            from .meter_exception_evidence import validate
            try:uploaded=validate(file)
            except ValueError as exc:abort(400,description=str(exc))
            if not db.session.execute(text('SELECT 1 FROM bil_meter_exception_evidence WHERE exception_id=:e AND sha256=:h'),dict(e=existing['id'],h=uploaded['sha256'])).first():abort(409,description='This incident was already saved. Open it and use Upload Evidence to add another file.')
        return existing['id']
    eid=db.session.execute(text('INSERT INTO bil_meter_exception(property_id,account_id,meter_id,exception_type,incident_date,notification_date,municipal_reference,description,remarks,replacement_meter_id,replacement_date,submission_key,created_by) VALUES(:p,:a,:m,:t,:d,:n,:ref,:description,:remarks,:r,:rd,:key,:u) RETURNING id'),values).scalar()
    event(eid,'created',values)
    if file and file.filename:evidence_add(p,dict(id=eid,account_id=acc['id']),file)
    return eid

def charges_ready():
    from .utilities_capture import available
    return available('bil_meter_exception_charge',['id','exception_id','statement_id','amount','bill_snapshot','submission_key']) and available('bil_meter_exception_charge_evidence',['charge_id','evidence_id'])

def charge_evidence(record,charge_id,evidence_id):
    if not charges_ready():abort(503,description='Continuing charge capture is temporarily unavailable. Please contact the administrator.')
    charge=db.session.execute(text('SELECT id FROM bil_meter_exception_charge WHERE id=:c AND exception_id=:e'),dict(c=identifier(charge_id,'charge'),e=record['id'])).scalar()
    evidence=db.session.execute(text('SELECT id FROM bil_meter_exception_evidence WHERE id=:v AND exception_id=:e'),dict(v=evidence_id,e=record['id'])).scalar()
    if not charge or not evidence:abort(403)
    added=db.session.execute(text('INSERT INTO bil_meter_exception_charge_evidence(charge_id,evidence_id) VALUES(:c,:v) ON CONFLICT DO NOTHING RETURNING charge_id'),dict(c=charge,v=evidence)).scalar()
    if added:event(record['id'],'charge_evidence_added',dict(charge_id=charge,evidence_id=str(evidence)))

def create_charge(p,form,file=None):
    property_scope(p,lock=True);record=scoped(p,form.get('exception_id'),True)
    if str(record['account_id'])!=form.get('account_id'):abort(403)
    if not charges_ready():abort(503,description='Continuing charge capture is temporarily unavailable. Please contact the administrator.')
    statement=db.session.execute(text('SELECT * FROM bil_municipal_statement WHERE id=:s AND account_id=:a FOR SHARE'),dict(s=identifier(form.get('statement_id'),'municipal bill'),a=record['account_id'])).mappings().first()
    if not statement:abort(400,description='Select a captured Metro Reconciliation bill belonging to this municipal account.')
    from .utilities_capture import amount
    value=amount(form.get('amount'),True)
    if value<=0:abort(400,description='The disputed amount must be positive. This record does not change municipal balances.')
    try:key=uuid.UUID(form.get('submission_key',''))
    except (ValueError,TypeError,AttributeError):abort(400,description='Reopen the charge form before saving.')
    values=dict(e=record['id'],s=statement['id'],a=value,ref=clean_text(form,'charge_reference',150),d=clean_text(form,'charge_description',2000,True),r=clean_text(form,'charge_remarks',5000),snapshot=json.dumps(dict(statement),default=str),key=key,u=current_user.id)
    previous=db.session.execute(text('SELECT * FROM bil_meter_exception_charge WHERE submission_key=:k'),dict(k=key)).mappings().first()
    if previous:
        expected=dict(exception_id=values['e'],statement_id=values['s'],amount=values['a'],charge_reference=values['ref'],description=values['d'],remarks=values['r'],created_by=values['u'])
        if any(previous[k]!=v for k,v in expected.items()):abort(409,description='This submission already recorded a different charge. Reopen the form.')
        if file and file.filename:
            from .meter_exception_evidence import validate
            try:meta=validate(file)
            except ValueError as exc:abort(400,description=str(exc))
            if not db.session.execute(text('SELECT 1 FROM bil_meter_exception_charge_evidence l JOIN bil_meter_exception_evidence v ON v.id=l.evidence_id WHERE l.charge_id=:c AND v.sha256=:h'),dict(c=previous['id'],h=meta['sha256'])).first():abort(409,description='This charge was already saved. Use Upload Evidence to add another document.')
        return record['id']
    charge=db.session.execute(text('INSERT INTO bil_meter_exception_charge(exception_id,statement_id,amount,charge_reference,description,remarks,bill_snapshot,submission_key,created_by) VALUES(:e,:s,:a,:ref,:d,:r,CAST(:snapshot AS json),:key,:u) RETURNING id'),values).scalar()
    event(record['id'],'charge_recorded',dict(charge_id=charge,statement_id=statement['id'],amount=str(value),bill_version=statement['version']))
    if file and file.filename:
        evidence_id=evidence_add(p,record,file);charge_evidence(record,charge,evidence_id)
    return record['id']

def evidence_download(p,evidence_id):
    property_scope(p);require_ready()
    row=db.session.execute(text('SELECT v.*,e.property_id,e.account_id FROM bil_meter_exception_evidence v JOIN bil_meter_exception e ON e.id=v.exception_id JOIN bil_muni_account a ON a.id=e.account_id WHERE v.id=:v AND e.property_id=:p AND a.property_id=:p'),dict(v=evidence_id,p=p)).mappings().first()
    if not row:abort(404)
    from .meter_exception_evidence import retrieve
    try:data=retrieve(row)
    except Exception:
        current_app.logger.exception('Private meter evidence retrieval failed');abort(503,description='Evidence could not be retrieved securely. Please contact the administrator.')
    response=send_file(BytesIO(data),mimetype=row['mime_type'],as_attachment=True,download_name=row['filename'])
    response.headers['Cache-Control']='private, no-store';response.headers['X-Content-Type-Options']='nosniff'
    return response

def page(p):
    property_scope(p)
    prop=db.session.execute(text('SELECT id,name FROM bil_property WHERE id=:p'),dict(p=p)).mappings().one()
    values=request.form.to_dict() if request.method=='POST' else {}
    error=None;status=200;detail=None
    if request.method=='POST':
        try:
            if request.args.get('account_id') and request.args['account_id']!=values.get('account_id'):abort(403)
            action=values.get('action')
            if action=='create':eid=create(p,values,request.files.get('evidence'))
            elif action=='evidence':
                property_scope(p,lock=True);detail=scoped(p,values.get('exception_id'),True)
                if str(detail['account_id'])!=values.get('account_id'):abort(403)
                if values.get('charge_id'):
                    if not charges_ready() or not db.session.execute(text('SELECT 1 FROM bil_meter_exception_charge WHERE id=:c AND exception_id=:e'),dict(c=identifier(values['charge_id'],'charge'),e=detail['id'])).first():abort(403)
                eid=detail['id'];evidence_id=evidence_add(p,detail,request.files.get('evidence'))
                if values.get('charge_id'):charge_evidence(detail,values['charge_id'],evidence_id)
            elif action=='charge':eid=create_charge(p,values,request.files.get('evidence'))
            else:abort(400,description='Select a valid exception action.')
            db.session.commit();flash('Meter exception saved. Existing meters, readings and municipal balances were preserved.','success')
            return redirect(url_for('billing_bp.meter_exceptions',property_id=p,account_id=values.get('account_id'),exception_id=eid,billing_month=request.values.get('billing_month','')))
        except HTTPException as exc:
            db.session.rollback()
            if exc.code in (403,404):raise
            error=exc.description;status=exc.code
        except Exception:
            db.session.rollback();current_app.logger.exception('Meter exception save failed');error='The exception could not be saved. No changes were committed.';status=503
    acc=account(p,request.values['account_id']) if request.values.get('account_id') else None
    meters=account_meters(p,acc) if acc else [dict(r) for r in db.session.execute(text('SELECT * FROM bil_meter WHERE property_id=:p ORDER BY meter_number,id'),dict(p=p)).mappings()]
    legacy=[m for m in meters if (m.get('status') or 'active').lower()!='active']
    entries=[];evidence=[];charges=[];bills=[]
    if ready():
        entries=[dict(r) for r in db.session.execute(text('SELECT e.*,a.account_number,m.meter_number FROM bil_meter_exception e JOIN bil_muni_account a ON a.id=e.account_id JOIN bil_meter m ON m.id=e.meter_id WHERE e.property_id=:p AND a.property_id=:p AND (:a IS NULL OR e.account_id=:a) ORDER BY e.incident_date DESC,e.id DESC'),dict(p=p,a=acc['id'] if acc else None)).mappings()]
        selected=request.values.get('exception_id')
        if selected:
            detail=scoped(p,selected)
            if acc and detail['account_id']!=acc['id']:abort(404)
            if not acc:acc=account(p,detail['account_id']);meters=account_meters(p,acc)
            evidence=[dict(r) for r in db.session.execute(text('SELECT id,filename,created_at FROM bil_meter_exception_evidence WHERE exception_id=:e ORDER BY created_at,id'),dict(e=detail['id'])).mappings()]
    if detail and charges_ready():
        charges=[dict(r) for r in db.session.execute(text('SELECT * FROM bil_meter_exception_charge WHERE exception_id=:e ORDER BY created_at,id'),dict(e=detail['id'])).mappings()]
        for charge in charges:
            charge['evidence']=[dict(r) for r in db.session.execute(text('SELECT v.id,v.filename FROM bil_meter_exception_charge_evidence l JOIN bil_meter_exception_evidence v ON v.id=l.evidence_id WHERE l.charge_id=:c AND v.exception_id=:e'),dict(c=charge['id'],e=detail['id'])).mappings()]
        bills=[dict(r) for r in db.session.execute(text('SELECT id,period,statement_date,balance,due FROM bil_municipal_statement WHERE account_id=:a ORDER BY statement_date DESC,id DESC'),dict(a=detail['account_id'])).mappings()]
    return render_template('program_billing/meter_exceptions.html',property=prop,accounts=accounts(p),account=acc,meters=meters,legacy=legacy,entries=entries,detail=detail,evidence=evidence,charges=charges,bills=bills,charges_available=charges_ready(),charge_submission_key=values.get('submission_key') if values.get('action')=='charge' else str(uuid.uuid4()),types=TYPES,available=ready(),values=values,submission_key=values.get('submission_key') or str(uuid.uuid4()),error=error,utility_month=request.values.get('billing_month','')),status
