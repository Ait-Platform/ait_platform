"""Private, append-only-in-application lease documents, amendments and mail audit."""
from copy import deepcopy
from datetime import datetime,timezone
from hashlib import sha256
from io import BytesIO
import json,re,uuid
from zipfile import ZipFile
from lxml import etree as ET
from itsdangerous import URLSafeTimedSerializer,BadSignature,SignatureExpired
from flask import abort,request,redirect,url_for,flash,send_file,current_app
from flask_login import current_user
from sqlalchemy import text
from app.extensions import db
from app.program_billing.routes import billing_bp
from app.program_billing import lease_snapshot as snapshot,lease_workflow as workflow,lease_preview as preview
from app.program_billing.lease_management import render_pm_template
from app.program_billing.lease_merge import W

KINDS={'signed_original':'Original lease ? signed copy, not verified','signed_addendum':'Addendum ? signed copy, not verified',
    'signed_termination':'Mutual termination ? signed copy, not verified'}
MIME='application/vnd.openxmlformats-officedocument.wordprocessingml.document'


def now():return datetime.now(timezone.utc).isoformat()


def context(property_id,lock=False):
    prop,reason,draft=workflow.selected_context(property_id,lock)
    if reason:abort(503,description=reason)
    if not draft:abort(400,description='Select an existing saved lease draft first.')
    return prop,draft


def ledger(draft):return deepcopy(draft['items'].get('_lease_history',{'documents':[],'emails':[]}))


def update_locked(draft,items):
    updated=db.session.execute(text("""UPDATE bil_lease_draft SET items=CAST(:items AS jsonb),version=version+1,updated_at=now()
        WHERE id=:d AND property_id=:p AND manager_id=:u AND version=:v RETURNING id"""),
        {'items':json.dumps(items),'d':draft['id'],'p':draft['property_id'],'u':current_user.id,'v':draft['version']}).scalar_one_or_none()
    if updated is None:abort(409,description='The lease changed. Reload before trying again.')
    db.session.commit()


def locked(property_id,draft_id):
    snapshot.authorised_property(property_id,True)
    return snapshot.draft_row(property_id,draft_id,True)


def document(draft,identifier):
    try:identifier=uuid.UUID(str(identifier))
    except (ValueError,TypeError):abort(404)
    meta=next((entry for entry in ledger(draft)['documents'] if entry['id']==str(identifier)),None)
    row=db.session.execute(text("""SELECT * FROM bil_lease_document WHERE id=:id AND draft_id=:d
        AND property_id=:p AND manager_id=:u AND state='available' AND verified_at IS NOT NULL"""),
        {'id':identifier,'d':draft['id'],'p':draft['property_id'],'u':current_user.id}).mappings().first()
    if row is None:abort(404)
    if meta is None:meta={'id':str(identifier),'kind':'legacy_attachment','filename':row['filename'],'sha256':row['sha256']}
    if meta['sha256']!=row['sha256']:abort(409,description='Document history and stored digest do not match.')
    from app.program_billing import lease_storage as storage
    try:payload=storage.retrieve(row)
    except Exception as error:
        current_app.logger.warning('Lease history retrieval failed error=%s',type(error).__name__)
        abort(503,description='The selected private document could not be retrieved and verified.')
    return dict(row),meta,payload


def originals(draft):
    return [entry for entry in ledger(draft)['documents'] if entry['kind']=='signed_original' and entry.get('state')=='available']


def store(draft,kind,filename,mime,payload,metadata,*,continuation=False,payload_builder=None):
    """Reserve immutable metadata and a unique object, verify bytes, then publish.

    Pending/failed rows are never downloadable; earlier versions remain intact.
    Locks/version predicates prevent concurrent lost updates to draft JSONB.
    """
    from app.program_billing import lease_storage as storage
    storage.destination()
    history=ledger(draft)
    if len(history['documents'])>=100:abort(409,description='Document history limit reached. Preserve existing documents before further uploads.')
    identifier=uuid.uuid4();extension='.docx' if mime==MIME else '.pdf'
    key=storage.key_for(draft['property_id'],draft['id'],identifier,extension)
    version=1+max((entry['version'] for entry in history['documents'] if entry['kind']==kind),default=0)
    metadata=deepcopy(metadata)
    if kind=='addendum_draft':
        metadata['addendum_number']=1+max((entry.get('addendum_number',entry['version']) for entry in history['documents'] if entry['kind']==kind and entry.get('original_id')==metadata.get('original_id')),default=0)
    if payload_builder:payload=payload_builder(metadata.get('addendum_number',version))
    if not payload or len(payload)>storage.MAX_BYTES:raise ValueError('Document exceeds the 10 MB storage limit.')
    entry=dict(metadata,id=str(identifier),kind=kind,version=version,uploaded_by=int(current_user.id),uploaded_at=now(),
        state='pending',signature_status='not-verified',filename=filename,sha256=sha256(payload).hexdigest())
    history['documents'].append(entry)
    items=deepcopy(draft['items']);items['_lease_history']=history
    db.session.execute(text("""INSERT INTO bil_lease_document
        (id,draft_id,property_id,manager_id,item_key,storage_key,filename,mime_type,size_bytes,sha256)
        VALUES (:id,:d,:p,:u,:item,:key,:filename,:mime,:size,:hash)"""),
        {'id':identifier,'d':draft['id'],'p':draft['property_id'],'u':current_user.id,'item':'history-'+kind,
         'key':key,'filename':filename,'mime':mime,'size':len(payload),'hash':entry['sha256']})
    if continuation:update_locked(draft,items)
    else:workflow.save_items(draft,items)
    record={'id':identifier,'draft_id':draft['id'],'property_id':draft['property_id'],'filename':filename,
        'storage_key':key,'size_bytes':len(payload),'sha256':entry['sha256']}
    try:
        storage.upload(key,payload,mime)
        if storage.retrieve(record)!=payload:raise ValueError('Storage verification failed')
        current=locked(draft['property_id'],draft['id']);items=deepcopy(current['items'])
        target=next(e for e in items['_lease_history']['documents'] if e['id']==str(identifier))
        target['state']='available'
        published=db.session.execute(text("UPDATE bil_lease_document SET state='available',verified_at=now() WHERE id=:id AND state='pending' RETURNING id"),{'id':identifier}).scalar_one_or_none()
        if published is None:raise ValueError('This pending document was not published.')
        update_locked(current,items)
    except Exception as error:
        db.session.rollback()
        current_app.logger.warning('Lease history upload unconfirmed error=%s',type(error).__name__)
        # Keep pending metadata/object for reconciliation; never delete originals.
        abort(503,description='Upload was not confirmed. The pending document is not published; earlier versions remain unchanged.')
    return entry


def addendum_bytes(master,original,wording,number):
    """Three parts only: heading, verbatim supplied content and signatures."""
    from app.program_billing.lease_standard import paragraph,saved_particulars
    particulars=saved_particulars(original['particulars'])
    parties=particulars.get('_parties',{})
    tenants=parties.get('lessees',[]) or ([parties['lessee']] if parties.get('lessee') else [])
    values=workflow.stage5_placeholder_values(particulars)
    owners=particulars.get('_contracting_lessors',{}).get('identities',[])
    nodes=[paragraph('Addendum No. '+str(number)+' to Agreement of Lease','Title'),
        paragraph('DRAFT \u2014 FOR REVIEW; NOT APPROVED OR ISSUED'),
        paragraph('Original lease: '+original['lease_reference']),
        paragraph('Original signed-copy ID: '+original['id']+'\nSHA-256: '+original['sha256']),
        paragraph('Property: '+(values.get('[S1 PROPERTY NAME]') or '[PROPERTY]')+'\n'+(values.get('[S1 PREMISES ADDRESS]') or '[ADDRESS]')),
        paragraph('Landlords: '+(values.get('[S1 LEGAL LANDLORD(S)]') or '[LANDLORDS NOT SUPPLIED]')),
        paragraph('Tenants: '+(values.get('[S1 LEGAL TENANT]') or '[TENANTS NOT SUPPLIED]')+'\n'+(values.get('[TENANT REGISTRATION OR ID]') or '[TENANT ID]')),
        paragraph('Amendment Content','Heading1'),paragraph(wording),paragraph('Signatures','Heading1')]
    def signature(label,name,identity,representative=None):
        text=label+': '+(name or '[LEGAL NAME]')+'\nID / registration number: '+(identity or '[ID / REGISTRATION NUMBER]')
        if representative and representative.get('signatory_name'):
            text+='\nRecorded representative: '+representative['signatory_name']
            text+='\nRepresentative ID: '+(representative.get('signatory_identity_number') or '[REPRESENTATIVE ID]')
            text+='\nCapacity: '+(representative.get('signatory_capacity') or '[CAPACITY]')
        text+='\nSignature: ____________________\nPlace: ____________________\nDate: ____________________'
        nodes.append(paragraph(text))
    for owner in owners or [{}]:signature('For the Landlord',owner.get('name',''),owner.get('identity_number',''),parties.get('lessor'))
    for tenant in tenants or [{}]:signature('For the Tenant',tenant.get('legal_name',''),tenant.get('identity_number',''),tenant if tenant.get('party_type')!='natural_person' else None)
    with ZipFile(BytesIO(master)) as archive:
        root=ET.fromstring(archive.read('word/document.xml'),parser=ET.XMLParser(resolve_entities=False,no_network=True))
        body=root.find('{'+W+'}body')
        if body is None:raise ValueError('Master has no document body.')
        section=body.find('{'+W+'}sectPr')
        for child in list(body):body.remove(child)
        for node in nodes:body.append(node)
        if section is not None:body.append(section)
        result=BytesIO()
        with ZipFile(result,'w') as output:
            for entry in archive.infolist():
                content=archive.read(entry.filename)
                if entry.filename=='word/document.xml':content=ET.tostring(root,encoding='utf-8',xml_declaration=True)
                elif re.fullmatch(r'word/(header|footer)\d+\.xml',entry.filename):
                    node=ET.fromstring(content,parser=ET.XMLParser(resolve_entities=False,no_network=True))
                    for item in node.iter('{'+W+'}t'):item.text=''
                    content=ET.tostring(node,encoding='utf-8',xml_declaration=True)
                output.writestr(entry,content)
    return result.getvalue()


@billing_bp.route('/billing/leases/property/<int:property_id>/history',methods=['GET','POST'],endpoint='lease_history')
@snapshot.endpoint
def history(property_id):
    prop,draft=context(property_id)
    if request.method=='POST' and request.form.get('action') in ('sc_pdf_preview','sc_pdf_confirm','record_signature_review'):
        from flask import jsonify
        return jsonify(error='This old upload form is no longer used. Select Upload / Replace PDF on Output.'),410
    if request.method=='POST' and request.form.get('action') in ('sc_pdf_upload',):
        from werkzeug.exceptions import HTTPException
        from flask import jsonify
        try:
            from .lease_sc_pdf import handle
            return handle(prop,draft)
        except HTTPException as error:
            db.session.rollback();return jsonify(error=error.description),error.code
    if request.method=='POST' and request.form.get('action') in ('sc_import_preview','sc_import_confirm','map_upload'):
        from werkzeug.exceptions import HTTPException
        from flask import jsonify
        try:
            if request.form['action']=='map_upload':
                from .lease_map_annexure import upload
                return upload(prop,draft)
            from .lease_sc_import import handle
            return handle(prop,draft)
        except HTTPException as error:
            db.session.rollback()
            return jsonify(error=error.description),error.code
    if request.method=='POST':
        # Preserve old upload-form submissions without duplicating the upload handler.
        return redirect(url_for('billing_bp.lease_signed_copy',property_id=property_id,draft_id=draft['id']),code=307)
    from .lease_kit import action_documents
    return render_pm_template('program_billing/lease_history.html',title='Output',property=prop,lease_draft=draft,
        action_documents=action_documents(property_id,draft['id']),unavailable=None)


@billing_bp.route('/billing/leases/property/<int:property_id>/signed-copy',methods=['GET','POST'],endpoint='lease_signed_copy')
@snapshot.endpoint
def signed_copy(property_id):
    prop,draft=context(property_id,request.method=='POST');errors=[]
    if request.method=='POST':
        from app.program_billing import lease_storage as storage
        request.max_content_length=storage.MAX_BYTES+1024*1024
        try:
            storage.destination()
            kind=request.form.get('kind')
            if kind not in KINDS:raise ValueError('Choose a supported signed-document type.')
            reference=request.form.get('lease_reference','').strip()
            if not reference or len(reference)>300:raise ValueError('Identify the specific original agreement (maximum 300 characters).')
            metadata={'lease_reference':reference}
            if kind!='signed_original':
                _,original,_=document(draft,request.form.get('original_id'))
                if original['kind']!='signed_original':raise ValueError('Select the original signed lease being amended or terminated.')
                metadata['original_id']=original['id']
            else:metadata['particulars']=deepcopy({k:v for k,v in draft['items'].items() if k not in ('_lease_history','_generations','_ait_standard')})
            upload=request.files.get('signed_document')
            if upload is None:raise ValueError('Choose a signed PDF or DOCX copy.')
            filename,ext,mime,payload,hash_value=storage.validate_upload(upload)
            store(draft,kind,filename,mime,payload,metadata)
            flash('Document stored and verified. Signatures and legal validity have not been verified.','success')
            return redirect(url_for('billing_bp.lease_signed_copy',property_id=property_id,draft_id=draft['id']))
        except ValueError as error:errors.append(str(error))
    entries=[entry for entry in ledger(draft)['documents'] if entry.get('kind')!='map_annexure']
    known={e['id'] for e in entries}
    for row in db.session.execute(text('SELECT id,filename,state,created_at,sha256,item_key FROM bil_lease_document WHERE draft_id=:d AND property_id=:p AND manager_id=:u'),{'d':draft['id'],'p':property_id,'u':current_user.id}).mappings():
        if row.get('item_key') in ('map-annexure','special-conditions-pdf'):continue
        if str(row['id']) not in known:entries.append({'id':str(row['id']),'filename':row['filename'],'kind':'legacy_attachment','version':None,'uploaded_by':None,'uploaded_at':row['created_at'].isoformat(),'state':row['state'],'signature_status':'not-verified','sha256':row['sha256']})
    entries=sorted(entries,key=lambda e:(e['uploaded_at'],e['id']))
    for entry in entries:
        if entry['state']=='available':entry['email_token']=signer().dumps({'kind':'document','document_id':entry['id'],'draft_id':str(draft['id']),'property_id':property_id,'version':draft['version'],'sha256':entry['sha256'],'nonce':str(uuid.uuid4())})
    return render_pm_template('program_billing/lease_signed_copy.html',title='Signed Copy',property=prop,lease_draft=draft,
        entries=entries,kinds=KINDS,originals=originals(draft),errors=errors,unavailable=None),400 if errors else 200


@billing_bp.route('/billing/leases/property/<int:property_id>/addendum',methods=['GET','POST'],endpoint='lease_addendum')
@snapshot.endpoint
def addendum(property_id):
    prop,draft=context(property_id,request.method=='POST');errors=[]
    arrangement_wording=''
    if request.method=='GET' and request.args.get('utility_arrangement_id'):
        from app.program_billing.lease_property_payments import utility_record
        arrangement=utility_record(property_id,request.args['utility_arrangement_id'])
        arrangement_wording=arrangement['details']['utility_allocation_method']
        if arrangement['details'].get('utility_references'):arrangement_wording+='\n'+arrangement['details']['utility_references']
    if request.method=='POST':
        from app.program_billing import lease_storage as storage,lease_property_master as masters
        request.max_content_length=storage.MAX_BYTES+1024*1024
        try:
            wording=request.form.get('wording','')
            source_upload=request.files.get('wording_file')
            wording_source=None
            if source_upload and source_upload.filename:
                name,mime,raw,hash_value=storage.validate_master(source_upload)
                extracted='\n'.join(preview.block_text(b) for b in preview.extract_blocks(raw)).strip()
                if wording:raise ValueError('Enter wording or upload a wording document, not both.')
                wording=extracted;wording_source=(name,mime,raw)
            if not wording.strip() or len(wording)>30000:raise ValueError('Supply amendment wording (maximum 30000 characters).')
            original_row,original,original_payload=document(draft,request.form.get('original_id'))
            if original['kind']!='signed_original':raise ValueError('Select the specific original signed agreement.')
            problem=masters.readiness()
            if problem:raise ValueError(problem)
            master=db.session.execute(text("SELECT * FROM bil_property_lease_master WHERE property_id=:p AND state='available' AND verified_at IS NOT NULL ORDER BY version DESC LIMIT 1"),{'p':property_id}).mappings().first()
            if master is None:raise ValueError('A verified property master is required for the addendum layout.')
            layout=storage.retrieve_master(master)
            metadata={'original_id':original['id'],'original_sha256':original_row['sha256'],
                'wording':wording,'layout_master_id':str(master['id']),'layout_sha256':master['sha256'],
                'lease_reference':original['lease_reference'],'legal_status':'draft-not-approved'}
            if wording_source:
                name,mime,raw=wording_source
                entry=store(draft,'amendment_wording',name,mime,raw,{'original_id':original['id'],'lease_reference':original['lease_reference']})
                metadata['wording_document_id']=entry['id']
                draft=locked(property_id,draft['id'])
            entry=store(draft,'addendum_draft','Addendum-DRAFT.docx',MIME,None,metadata,continuation=bool(wording_source),
                payload_builder=lambda number:addendum_bytes(layout,original,wording,number))
            flash('Draft addendum generated and stored. The original agreement is unchanged.','success')
            return redirect(url_for('billing_bp.lease_history_document',property_id=property_id,document_id=entry['id'],draft_id=draft['id']))
        except ValueError as error:errors.append(str(error))
    return render_pm_template('program_billing/lease_addendum.html',title='Addendum',property=prop,lease_draft=draft,
        originals=originals(draft),arrangement_wording=arrangement_wording,errors=errors,unavailable=None),400 if errors else 200


def signer():return URLSafeTimedSerializer(current_app.config['SECRET_KEY'],salt='lease-view-version-001')


def view_token(draft,kind,payload,**identifiers):
    return signer().dumps(dict(identifiers,kind=kind,draft_id=str(draft['id']),property_id=draft['property_id'],
        version=draft['version'],sha256=sha256(payload).hexdigest(),nonce=str(uuid.uuid4())))


@billing_bp.get('/billing/leases/property/<int:property_id>/history/<uuid:document_id>',endpoint='lease_history_document')
@snapshot.endpoint
def view_document(property_id,document_id):
    prop,draft=context(property_id)
    row,meta,payload=document(draft,document_id)
    if row['mime_type']!=MIME:
        return send_file(BytesIO(payload),mimetype=row['mime_type'],as_attachment=True,download_name=row['filename'],max_age=0)
    blocks=preview.extract_blocks(payload)
    token=view_token(draft,'document',payload,document_id=str(document_id))
    return render_pm_template('program_billing/lease_live_view.html',title='View '+meta['filename'],property=prop,lease_draft=draft,
        lease_blocks=blocks,logical_pages=preview.logical_pages(blocks,blocks),viewer_only=True,document_token=token,
        back_url=url_for('billing_bp.lease_signed_copy',property_id=property_id,draft_id=draft['id']))


@billing_bp.get('/billing/leases/property/<int:property_id>/history/<uuid:document_id>/download',endpoint='lease_history_download')
@snapshot.endpoint
def download(property_id,document_id):
    prop,draft=context(property_id);row,meta,payload=document(draft,document_id)
    return send_file(BytesIO(payload),mimetype=row['mime_type'],as_attachment=True,download_name=row['filename'],max_age=0)


def resolve_token(property_id,token,lock=False):
    prop,draft=context(property_id,lock)
    try:selection=signer().loads(token,max_age=3600)
    except (BadSignature,SignatureExpired):abort(400,description='The document selection expired or was altered. Reopen View Lease.')
    if selection.get('property_id')!=property_id or selection.get('draft_id')!=str(draft['id']):abort(404)
    if selection['kind']=='document':
        row,meta,payload=document(draft,selection.get('document_id'));name=row['filename'];mime=row['mime_type']
    else:
        if selection.get('version')!=draft['version']:abort(409,description='Saved particulars changed. Reopen View Lease before emailing.')
        if selection['kind']=='standard':
            from app.program_billing import lease_standard as standard
            _,_,_,_,payload=standard.prepared(property_id)
        elif selection['kind']=='working':
            row,payload=preview.prepared_document(property_id,draft,selection.get('master_id'))
        else:abort(400)
        name='Lease-DRAFT.docx';mime=MIME
    if sha256(payload).hexdigest()!=selection.get('sha256'):abort(409,description='The displayed document changed. Reopen View Lease.')
    return prop,draft,selection,payload,name,mime


@billing_bp.route('/billing/leases/property/<int:property_id>/email',methods=['GET','POST'],endpoint='lease_document_email')
@snapshot.endpoint
def email_document(property_id):
    token=request.values.get('document_token','')
    prop,draft,selection,payload,filename,mime=resolve_token(property_id,token,request.method=='POST')
    errors=[]
    recipient=request.form.get('recipient',getattr(current_user,'email','') or '').strip()
    message=request.form.get('message','').strip()
    if request.method=='POST':
        from email_validator import validate_email,EmailNotValidError
        try:recipient=validate_email(recipient,check_deliverability=False).normalized
        except EmailNotValidError:errors.append('Enter a valid recipient email address.')
        if len(message)>5000:errors.append('The optional message must not exceed 5000 characters.')
        if not errors:
            history=ledger(draft)
            if len(history['emails'])>=100:abort(409,description='Email history limit reached.')
            if any(e['id']==selection['nonce'] for e in history['emails']):abort(409,description='This email action was already attempted. Reopen the document to send another email.')
            attempt={'id':selection['nonce'],'recipient':recipient,'by':int(current_user.id),'at':now(),
                'document_sha256':selection['sha256'],'document_id':selection.get('document_id'),'filename':filename,'status':'pending'}
            history['emails'].append(attempt);items=deepcopy(draft['items']);items['_lease_history']=history
            update_locked(draft,items)  # Reserve before sending; never automatically resend pending attempts.
            accepted=False
            try:
                from app.utils.mailer import send_standard_report_email
                accepted=send_standard_report_email(to_email=recipient,subject='AIT Lease Document',pdf_url='',pdf_bytes=payload,
                    filename=filename,user_name=getattr(current_user,'name',None),attachment_mime=mime,message=message,document_label='lease document') is True
            except Exception as error:current_app.logger.warning('Lease email submission failed error=%s',type(error).__name__)
            current=locked(property_id,draft['id']);items=deepcopy(current['items'])
            target=next(e for e in items['_lease_history']['emails'] if e['id']==attempt['id'])
            target['status']='submitted' if accepted else 'failed';target['completed_at']=now()
            update_locked(current,items)
            if accepted:
                flash('Document submitted successfully to '+recipient+'. Delivery is not guaranteed.','success')
                return redirect(url_for('billing_bp.lease_history',property_id=property_id,draft_id=draft['id']))
            errors.append('The email sender did not confirm submission. No success was recorded. Reopen the document before retrying.')
    return render_pm_template('program_billing/lease_document_email.html',title='Email Lease Document',property=prop,lease_draft=draft,
        document_token=token,recipient=recipient,message=message,errors=errors,unavailable=None),400 if errors else 200


@billing_bp.after_request
def private_history(response):
    if request.endpoint in {'billing_bp.lease_history','billing_bp.lease_signed_copy','billing_bp.lease_addendum','billing_bp.lease_history_document',
        'billing_bp.lease_history_download','billing_bp.lease_document_email'}:
        response.headers['Cache-Control']='private, no-store';response.headers['X-Content-Type-Options']='nosniff'
        response.headers['Referrer-Policy']='same-origin'
    return response
