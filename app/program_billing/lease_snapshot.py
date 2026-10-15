"""R01 snapshot workflow. All persistence remains behind explicit schema approval."""
import json,uuid
from datetime import date
from functools import wraps
from pathlib import Path
from io import BytesIO
from flask import abort,current_app,request,redirect,url_for,flash,send_file
from sqlalchemy import text,inspect
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.exceptions import HTTPException
from app.extensions import db
from app.program_billing.routes import billing_bp
from app.program_billing.lease_management import require_pm_user,render_pm_template
from app.program_billing.lease_documents import EXPECTED_SHA256,WORD_NAME
from flask_login import current_user

CATALOG=json.loads(Path(__file__).with_name('lease_snapshot_catalog.json').read_text(encoding='utf-8-sig'))
if CATALOG['source_docx_sha256']!=EXPECTED_SHA256[WORD_NAME]:raise RuntimeError("R01 snapshot catalog/source mismatch")
ITEMS={item['key']:item for item in CATALOG['items']}

def readiness():
    if current_app.config.get('PM_LEASE_DRAFT_WRITES_APPROVED') is not True:
        return "Persistent lease draft storage is not approved. Saving and uploads are disabled."
    try:
        inspector=inspect(db.session.connection())
        required={'bil_lease_draft':{'id','property_id','manager_id','items','version','source_version','registration_token','status','created_at','updated_at'},
            'bil_lease_document':{'id','draft_id','property_id','manager_id','item_key','state','storage_key','sha256','size_bytes','filename','mime_type','created_at','verified_at'}}
        for table,columns in required.items():
            if not inspector.has_table(table) or not columns<={c['name'] for c in inspector.get_columns(table)}:
                return "The approved lease draft/document schema is not available."
        if not inspector.has_table('alembic_version') or 'pm_lease_001' not in db.session.execute(text('SELECT version_num FROM alembic_version')).scalars().all():
            return "Lease migration pm_lease_001 has not been verified in Alembic history."
        return None
    except SQLAlchemyError:
        return "Lease database readiness could not be verified."

def endpoint(function):
    @wraps(function)
    def wrapped(*args,**kwargs):
        require_pm_user()
        try:return function(*args,**kwargs)
        except HTTPException:
            db.session.rollback();raise
        except SQLAlchemyError as error:
            db.session.rollback()
            current_app.logger.error("Lease draft operation failed error=%s",type(error).__name__)
            abort(503,description="Lease draft changes could not be saved. No success was confirmed.")
    return wrapped

def authorised_property(property_id,lock=False):
    row=db.session.execute(text('SELECT id,name,address FROM bil_property WHERE id=:p AND manager_id=:u AND is_archived=false'+(' FOR UPDATE' if lock else '')),
        {'p':property_id,'u':current_user.id}).mappings().first()
    if row is None:abort(404)
    return row

def require_schema():
    reason=readiness()
    if reason:abort(503,description=reason)

def draft_row(property_id,draft_id,lock=False):
    try:identifier=uuid.UUID(str(draft_id))
    except (ValueError,TypeError):abort(404)
    row=db.session.execute(text('''SELECT d.* FROM bil_lease_draft d JOIN bil_property p ON p.id=d.property_id
        WHERE d.id=:d AND d.property_id=:p AND d.manager_id=:u AND p.manager_id=:u AND p.is_archived=false
        '''+('FOR UPDATE OF d' if lock else '')),{'d':identifier,'p':property_id,'u':current_user.id}).mappings().first()
    if row is None:abort(404)
    if row['status']!='draft' or row['source_version']!=CATALOG['version'] or not isinstance(row['items'],dict):
        abort(409,description="This draft uses a different document version and requires review.")
    return row

def documents(draft):
    if not draft:return []
    return db.session.execute(text('''SELECT id,filename,item_key,state,created_at FROM bil_lease_document
        WHERE draft_id=:d AND property_id=:p AND manager_id=:u ORDER BY created_at,id'''),
        {'d':draft['id'],'p':draft['property_id'],'u':current_user.id}).mappings().all()

def item_status(item,data):
    if not item['editable']:return "Document review required"
    if data.get('not_applicable') and item['requirement']=='Conditional':return "Not Applicable declared - review pending"
    value=data.get('value','')
    if value and not any(token in value for token in item.get('placeholders',[])):return "Information recorded"
    return "Not supplied (optional)" if item['requirement']=='Optional' else "Information outstanding"

def snapshot_context(property_id):
    reason=readiness();draft=None;drafts=[]
    if not reason:
        drafts=db.session.execute(text('''SELECT d.id,d.updated_at FROM bil_lease_draft d JOIN bil_property p ON p.id=d.property_id
            WHERE d.property_id=:p AND d.manager_id=:u AND p.manager_id=:u AND p.is_archived=false ORDER BY d.updated_at DESC,d.id'''),
            {'p':property_id,'u':current_user.id}).mappings().all()
        requested=request.args.get('draft_id')
        if requested:draft=draft_row(property_id,requested)
        elif drafts:draft=draft_row(property_id,drafts[0]['id'])
    files=documents(draft)
    entries=[]
    for item in CATALOG['items']:
        data=draft['items'].get(item['key'],{}) if draft else {}
        completion=item_status(item,data)
        if item['key'].startswith('annexure-') and not data.get('not_applicable'):
            attached=any(f['item_key']==item['key'] and f['state']=='available' for f in files)
            if completion=='Information recorded':completion='Details and attachment recorded' if attached else 'Details recorded; attachment outstanding'
        entries.append(dict(item,summary=data.get('reason') if data.get('not_applicable') else data.get('value',''),
            completion=completion))
    return dict(snapshot_items=entries,lease_draft=draft,lease_drafts=drafts,draft_unavailable=reason,
        draft_token=str(uuid.uuid4()),lease_files=files)

@billing_bp.post('/billing/leases/property/<int:property_id>/drafts',endpoint='lease_draft_create')
@endpoint
def create_draft(property_id):
    authorised_property(property_id,True);require_schema()
    try:token=uuid.UUID(request.form.get('registration_token',''))
    except ValueError:abort(400,description="Reload the workspace before starting a draft.")
    identifier=uuid.uuid4()
    row=db.session.execute(text('''INSERT INTO bil_lease_draft(id,property_id,manager_id,registration_token,source_version)
        VALUES (:d,:p,:u,:t,:v) ON CONFLICT (manager_id,registration_token) DO NOTHING RETURNING id'''),
        {'d':identifier,'p':property_id,'u':current_user.id,'t':token,'v':CATALOG['version']}).scalar_one_or_none()
    if row is None:
        row=db.session.execute(text('SELECT id FROM bil_lease_draft WHERE manager_id=:u AND registration_token=:t AND property_id=:p'),
            {'u':current_user.id,'t':token,'p':property_id}).scalar_one_or_none()
        if row is None:abort(409)
    db.session.commit();flash("Lease draft created. This is not legal approval or an active lease.","success")
    return redirect(url_for('billing_bp.pm_lease_management',property_id=property_id,draft_id=row))

@billing_bp.route('/billing/leases/property/<int:property_id>/snapshot/<item_key>',methods=['GET','POST'],endpoint='lease_snapshot_item')
@endpoint
def edit_item(property_id,item_key):
    property_record=authorised_property(property_id,request.method=='POST')
    item=ITEMS.get(item_key)
    if item is None:abort(404)
    reason=readiness();draft=None
    if not reason and request.values.get('draft_id'):draft=draft_row(property_id,request.values['draft_id'],request.method=='POST')
    data=draft['items'].get(item_key,{}) if draft else {}
    error=None;status=200
    if request.method=='POST':
        require_schema()
        if not item['editable'] or not draft:abort(400,description="Select a persistent editable draft first.")
        value=request.form.get('value','').strip()
        note=request.form.get('reason','').strip()
        na=request.form.get('not_applicable')=='on'
        data=dict(value=value,reason=note,not_applicable=na)
        if len(value)>4000 or len(note)>500:error="Keep details within 4000 characters and the explanation within 500."
        elif na and item['requirement']!='Conditional':error="Not Applicable is allowed only for conditional items."
        elif na and len(note)<10:error="Explain why this conditional item does not apply (at least 10 characters)."
        elif not na and not value and item['requirement']!='Optional':error="Enter the captured particulars; they will remain subject to review."
        elif not na and any(token in value for token in item.get('placeholders',[])):error="Replace the R01 placeholders with captured details before saving."
        elif not na and item.get('input_type')=='date':
            try:date.fromisoformat(value)
            except ValueError:error="Enter a valid date."
        if not error and not na and item.get('input_type')=='date':
            dates={i['label']:(value if i['key']==item_key else draft['items'].get(i['key'],{}).get('value',''))
                for i in CATALOG['items'] if i.get('input_type')=='date'}
            if dates.get('Commencement date') and dates.get('Expiry date'):
                try:
                    if date.fromisoformat(dates['Expiry date'])<date.fromisoformat(dates['Commencement date']):
                        error="Expiry date cannot precede commencement."
                except ValueError:error="Resolve invalid saved lease dates before saving."
        if not error:
            try:version=int(request.form.get('version',''))
            except ValueError:abort(400)
            items=dict(draft['items']);items[item_key]=data
            updated=db.session.execute(text('''UPDATE bil_lease_draft SET items=CAST(:items AS jsonb),version=version+1,updated_at=now()
                WHERE id=:d AND property_id=:p AND manager_id=:u AND version=:v RETURNING id'''),
                {'items':json.dumps(items),'d':draft['id'],'p':property_id,'u':current_user.id,'v':version}).scalar_one_or_none()
            if updated is None:abort(409,description="The draft changed. Reload this item before saving.")
            db.session.commit();flash("Lease particulars saved. Data completion is not legal approval.","success")
            return redirect(url_for('billing_bp.pm_lease_management',property_id=property_id,draft_id=draft['id']))
        status=400
    return render_pm_template('program_billing/lease_snapshot_item.html',property=property_record,item=item,
        data=data,lease_draft=draft,unavailable=reason,error=error),status

@billing_bp.route('/billing/leases/property/<int:property_id>/documents',methods=['GET','POST'],endpoint='lease_draft_documents')
@endpoint
def document_page(property_id):
    property_record=authorised_property(property_id,request.method=='POST')
    reason=readiness();draft=None
    if not reason and request.values.get('draft_id'):draft=draft_row(property_id,request.values['draft_id'],request.method=='POST')
    from app.program_billing import lease_storage as storage
    storage_reason=None
    try:storage.destination()
    except ValueError as error:storage_reason=str(error)
    if request.method=='POST':
        require_schema()
        if not draft:abort(400,description="Select a persistent draft first.")
        if storage_reason:abort(503,description=storage_reason)
        item_key=request.form.get('item_key','lease-document')
        if item_key!='lease-document' and not item_key.startswith('annexure-'):abort(400)
        if item_key!='lease-document' and item_key not in ITEMS:abort(400)
        file=request.files.get('document')
        if not file:abort(400,description="Select a PDF or DOCX.")
        try:filename,extension,mime,payload,digest=storage.validate_upload(file)
        except ValueError as error:abort(400,description=str(error))
        identifier=uuid.uuid4();key=storage.key_for(property_id,draft['id'],identifier,extension)
        db.session.execute(text('''INSERT INTO bil_lease_document(id,draft_id,property_id,manager_id,item_key,storage_key,filename,mime_type,size_bytes,sha256)
            VALUES(:id,:d,:p,:u,:item,:key,:name,:mime,:size,:sha)'''),
            {'id':identifier,'d':draft['id'],'p':property_id,'u':current_user.id,'item':item_key,'key':key,
             'name':filename,'mime':mime,'size':len(payload),'sha':digest})
        # Durable pending metadata first; failed/uncertain uploads are never downloadable.
        db.session.commit()
        try:
            storage.upload(key,payload,mime)
            authorised_property(property_id,True);draft_row(property_id,draft['id'],True)
            db.session.execute(text("UPDATE bil_lease_document SET state='available',verified_at=now() WHERE id=:id AND manager_id=:u"),
                {'id':identifier,'u':current_user.id})
            db.session.commit()
        except Exception as error:
            db.session.rollback()
            current_app.logger.error("Lease upload not confirmed error=%s",type(error).__name__)
            # Pending metadata is intentionally retained for reconciliation; no unsafe object deletion.
            abort(503,description="Upload/finalization was not confirmed. Check stored attachment status before retrying; metadata is retained for reconciliation.")
        flash("Lease document stored and retrieved successfully. It remains a draft attachment.","success")
        return redirect(url_for('billing_bp.lease_supporting',property_id=property_id,draft_id=draft['id']))
    return render_pm_template('program_billing/lease_snapshot_documents.html',property=property_record,
        lease_draft=draft,unavailable=reason or storage_reason,lease_files=documents(draft),
        annexures=[i for i in CATALOG['items'] if i['key'].startswith('annexure-')])

@billing_bp.get('/billing/leases/documents/<uuid:document_id>',endpoint='lease_document_get')
@endpoint
def document_get(document_id):
    require_schema()
    record=db.session.execute(text('''SELECT f.* FROM bil_lease_document f JOIN bil_lease_draft d ON d.id=f.draft_id
        JOIN bil_property p ON p.id=f.property_id WHERE f.id=:id AND f.manager_id=:u AND d.manager_id=:u
        AND p.manager_id=:u AND p.is_archived=false AND f.state='available' '''),
        {'id':document_id,'u':current_user.id}).mappings().first()
    if record is None:abort(404)
    from app.program_billing import lease_storage as storage
    try:payload=storage.retrieve(record)
    except Exception as error:
        current_app.logger.error("Lease download failed error=%s",type(error).__name__)
        abort(503,description="The private document could not be retrieved or verified.")
    inline=request.args.get('view')=='1' and record['mime_type']=='application/pdf'
    response=send_file(BytesIO(payload),mimetype=record['mime_type'],as_attachment=not inline,
        download_name=record['filename'],conditional=False,etag=False,max_age=0)
    response.headers['Cache-Control']='private, no-store'
    response.headers['X-Content-Type-Options']='nosniff'
    response.headers['Content-Security-Policy']="sandbox; default-src 'none'"
    return response

@billing_bp.after_request
def protect_snapshot_responses(response):
    if request.endpoint in {
        'billing_bp.lease_snapshot_item', 'billing_bp.lease_draft_documents',
        'billing_bp.lease_draft_create', 'billing_bp.lease_document_get',
    }:
        response.headers['Cache-Control'] = 'private, no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
    return response
