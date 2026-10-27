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
        action_documents=action_documents(property_id,draft['id']),addendum_documents=addendum_history(draft,property_id),unavailable=None)


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


CLAUSE_SOURCES={'full_lease':'Full Lease','certificate':'Certificate','core':'Core Lease','special_conditions':'Special Conditions','signature':'Signatures'}


def clause_lease(draft,identifier):
    """Only a signed original in this draft, or this draft when none is uploaded."""
    choices=originals(draft)
    identifier=identifier or (choices[0]['id'] if choices else 'draft')
    if identifier=='draft' and not choices:return identifier,draft['items'],'Current saved lease draft'
    original=next((entry for entry in choices if entry['id']==identifier),None)
    if original is None:abort(404)
    if not isinstance(original.get('particulars'),dict):return identifier,None,original['lease_reference']
    return identifier,original['particulars'],original['lease_reference']


def clause_catalog(items):
    """Actual fixed templates + saved particulars. No keyword-invented SC clauses."""
    from . import lease_certificate as certificate,lease_certificate_form as form,lease_kit_pdf,lease_signing
    from .lease_certificate_parties import blocks
    from .lease_supplementary_conditions import legacy_entries
    import fitz
    catalog=[];notices=[]
    def add(source,reference,description,wording,provenance):
        identifier=source+':'+sha256(str(reference).encode()).hexdigest()[:24]
        catalog.append(dict(id=identifier,source=source,source_label=CLAUSE_SOURCES[source],reference=str(reference),description=description,original=wording,source_hash=sha256(wording.encode()).hexdigest(),provenance=provenance))
    sections=form.render_rows(form.certificate_layout(*form.counts(items)),certificate.document_values(items))
    for section in sections:
        if section['title'].startswith('01'):continue
        for row in section['rows']:
            wording=''.join(str(part.get('value','')) if 'token' in part else part.get('text','') for part in row['parts'])
            add('certificate',row['label'],section['title'],wording,'Saved particulars / fixed Certificate field')
    for group in blocks(items):
        for party in group['parties']:
            reference=group['label']+' — '+(party.get('name') or party.get('identity_number') or '')
            wording=' | '.join(str(party.get(key) or '') for key in ('name','identity_number'))
            for representative in party.get('representatives',[]):wording+='\nRepresentative: '+' | '.join(str(representative.get(key) or '') for key in ('name','identity_number','capacity'))
            add('certificate',reference,'Contracting party',wording,'Saved contracting-party particulars')
    for party in lease_signing.rows(items):
        party_name=' | '.join(str(party.get(key) or '') for key in ('name','identity_number')).strip(' |') or party['role']
        for field,label in (('name','Signing party'),('place','Place of signing'),('date','Date of signing')):
            add('signature',party['role']+' — '+party_name+' — '+label,label,str(party.get(field) or ''),'Saved particulars / fixed Signatures field')
        for representative in party.get('representatives',[]):
            if representative.get('name'):add('signature',party['role']+' — '+party_name+' — Authorised representative','Authorised representative',' | '.join(str(representative.get(key) or '') for key in ('name','identity_number')),'Saved particulars / fixed Signatures field')
        add('signature',party['role']+' — '+party_name+' — Signature','Signature','________________________________','Fixed Signatures template')
    try:
        core=lease_kit_pdf.CORE.read_bytes()
        if sha256(core).hexdigest()!=lease_kit_pdf.CORE_SHA256:raise ValueError('Approved Core hash mismatch')
        parsed=fitz.open(stream=core,filetype='pdf');entries=[];active=None
        for page in parsed:
            for block in page.get_text('dict')['blocks']:
                for line in block.get('lines',[]):
                    value=''.join(span['text'] for span in line['spans'])
                    if value.strip()=='AIT Residential Core | FINAL REVIEW' or re.fullmatch(r'Final review \| Page \d+',value.strip()):continue
                    match=re.match(r'^(\d+\.\d+(?:\.\d+)*)\s+(.+)',value)
                    heading=re.match(r'^(\d+)\s+(.+)',value) if any('bold' in span.get('font','').lower() for span in line['spans']) else None
                    if match or heading:
                        found=match or heading;active=[found[1],found[2].strip(),value];entries.append(active)
                    elif active:active[2]+='\n'+value
        parsed.close()
        for reference,description,wording in entries:add('core',reference,description,wording,'Approved nine-page Core / SHA-256 '+lease_kit_pdf.CORE_SHA256)
    except (OSError,ValueError):notices.append('Approved Core clauses are unavailable; no substitute has been used.')
    if items.get('_sc_pdf_current'):
        notices.append('Current SC is an uploaded PDF. Automatic clause extraction is not assumed reliable. Open the current PDF, then supply the exact provision reference and original wording below; these entries are labelled user-supplied.')
    else:
        state=items.get('_supplementary_conditions',{})
        if state.get('mode')!='none':
            for index,entry in enumerate(state.get('clauses',[]),1):
                add('special_conditions',entry.get('number') or 'SC-%02d'%index,entry.get('heading',''),entry.get('text',''),'Saved Special Conditions clause')
            for entry in legacy_entries(items):
                add('special_conditions',entry['source'],entry.get('label','Saved Special Condition'),entry['text'],'Saved legacy Special Condition')
    return catalog,notices


def clause_relationships(catalog):
    """Only explicit references; no assumed SC numbering or semantic matching."""
    relationships=[];by_core={entry['reference']:entry for entry in catalog if entry['source']=='core'}
    certificate={entry['reference'].casefold():entry for entry in catalog if entry['source']=='certificate'}
    for entry in catalog:
        if entry.get('manual'):continue
        wording=entry['original']
        for match in re.finditer(r'\b(?:Core(?: Lease)?(?: clause)?|clause)\s+(\d+\.\d+(?:\.\d+)*)\b',wording,re.I):
            if entry['source']!='core' and not re.match(r'Core',match[0],re.I):continue
            target=by_core.get(match[1])
            if target and target['id']!=entry['id']:relationships.append(dict(left=entry['id'],right=target['id'],evidence='Explicit reference: '+match[0]))
        if entry['source']=='core' and re.search(r'\bCertificate\b',wording):
            for phrase in ('monthly rental','security deposit','commencement','renewal notice'):
                target=certificate.get(phrase)
                if target and re.search(r'\b'+re.escape(phrase)+r'\b',wording,re.I):relationships.append(dict(left=entry['id'],right=target['id'],evidence='Core '+entry['reference']+' expressly refers to the Certificate and '+phrase))
    distinct=[];seen=set()
    for relationship in relationships:
        pair=tuple(sorted((relationship['left'],relationship['right'])))
        if pair not in seen:distinct.append(relationship);seen.add(pair)
    return distinct


def clause_register_state(draft,identifier=None):
    key,source_items,label=clause_lease(draft,identifier)
    catalog,notices=clause_catalog(source_items) if source_items is not None else ([],['The selected original has no saved particulars snapshot. No later draft provisions have been substituted. Existing documents and history remain available.'])
    saved=deepcopy(draft['items'].get('_addendum_clause_registers',{}).get(key,{'schema_version':1,'selected':[],'manual':[]}))
    pdf_id=(source_items or {}).get('_sc_pdf_current')
    for entry in saved.get('manual',[]):
        if entry.get('pdf_id')==pdf_id:catalog.append(entry)
    by_id={entry['id']:entry for entry in catalog}
    selected=saved.get('selected',[])
    for entry in selected:
        current=by_id.get(entry['id'])
        entry['source_status']='Current source' if current and current['source_hash']==entry['source_hash'] else 'Source changed or unavailable; saved wording retained'
    return dict(lease_key=key,lease_label=label,catalog=catalog,notices=notices,selected=selected,relationships=clause_relationships(catalog),pdf_id=pdf_id)


def clause_register_save(prop,draft):
    from flask import jsonify
    state=clause_register_state(draft,request.form.get('clause_lease'))
    if request.form.get('register_version')!=str(draft['version']):abort(409,description='The lease or clause register changed. Reload this page before changing the selection; the saved register is unchanged.')
    items=deepcopy(draft['items']);registers=items.setdefault('_addendum_clause_registers',{})
    saved=deepcopy(registers.get(state['lease_key'],{'schema_version':1,'selected':[],'manual':[]}))
    selected={entry['id']:entry for entry in saved['selected']}
    if request.form['action']=='addendum_manual_clause':
        if not state['pdf_id']:abort(400,description='Manual transcription is available only for the selected lease’s uploaded SC PDF.')
        reference=request.form.get('reference','').strip();wording=request.form.get('original','');description=request.form.get('description','').strip()
        if not reference or len(reference)>200 or not wording.strip() or len(wording)>100000 or len(description)>500 or '\x00' in reference+wording+description:abort(400,description='Supply an exact reference (up to 200 characters) and original wording (up to 100000 characters).')
        identifier='manual-sc:'+sha256((str(state['pdf_id'])+'|'+reference).encode()).hexdigest()[:24]
        entry=dict(id=identifier,source='special_conditions',source_label='Special Conditions',reference=reference,description=description or reference,original=wording,source_hash=sha256(wording.encode()).hexdigest(),provenance='User-supplied transcription from uploaded SC PDF; not automatically verified',manual=True,pdf_id=state['pdf_id'])
        existing=next((old for old in saved['manual'] if old['id']==identifier),None)
        if existing and existing['original']!=wording:abort(409,description='That provision already has different original wording in the working register. The existing wording was preserved.')
        if not existing:saved['manual'].append(entry)
        selected[identifier]=existing or entry
    else:
        identifier=request.form.get('provision_id');checked=request.form.get('selected')
        if checked not in ('yes','no'):abort(400)
        if checked=='yes':
            entry=next((entry for entry in state['catalog'] if entry['id']==identifier),None)
            if entry is None:abort(404)
            existing=selected.get(identifier)
            if existing and existing['source_hash']!=entry['source_hash']:abort(409,description='The original provision changed. The existing selection was preserved; remove it explicitly before selecting the new wording.')
            selected[identifier]=deepcopy(entry)
        else:selected.pop(identifier,None)
    saved.update(selected=list(selected.values()),updated_at=now(),updated_by=int(current_user.id),lease_key=state['lease_key'])
    registers[state['lease_key']]=saved;update_locked(draft,items)
    fresh=locked(prop['id'],draft['id']);result=clause_register_state(fresh,state['lease_key']);result['version']=fresh['version']
    return jsonify(result)


def full_lease_identity(draft):
    available=originals(draft)
    original=available[-1] if available else None
    return (original['id'],original['lease_reference'],original) if original else ('draft','Lease draft '+str(draft['id']),None)


def full_lease_selection_state(draft):
    key,label,_=full_lease_identity(draft)
    saved=deepcopy(draft['items'].get('_lease_history',{}).get('full_lease_selections',{}).get(key,{}))
    selected=saved.get('selected',[])
    for entry in selected:entry['source_status']='Saved clause reference'
    return dict(lease_key=key,lease_label=label,selected=selected,version=draft['version'],catalog=[],relationships=[],notices=[],pdf_id=None,selection_mode='full_pdf')


def full_lease_index(prop,draft,with_pdf=False,existing_only=False):
    from .lease_clause_index import load,existing
    if existing_only:index,payload=existing(prop,draft),None
    else:index,payload=load(prop,draft,full_lease_provisions,with_pdf=with_pdf)
    draft['items'].setdefault('_lease_history',{}).update(full_lease_clause_index=index)
    return index,payload


def full_lease_source(prop,draft):
    # Explicit PDF reference request only; clause selection never calls this.
    from .lease_kit import preview_context
    from .lease_kit_pdf import build_pdf
    payload=build_pdf('lease',preview_context(prop['id']))
    return payload,sha256(payload).hexdigest()


def full_lease_provisions(payload,core_pages=9,section_pages=None):
    """Index actual PDF text; only explicit headings/field labels become references."""
    import fitz
    document=fitz.open(stream=payload,filetype='pdf');pages=[];notices=[]
    for index,page in enumerate(document):
        lines=[]
        try:blocks=page.get_text('dict',flags=fitz.TEXTFLAGS_DICT & ~fitz.TEXT_PRESERVE_IMAGES)['blocks']
        except (RuntimeError,ValueError,UnicodeError):
            notices.append('PDF page '+str(index+1)+' text could not be extracted and is not searchable.');pages.append([]);continue
        for block in blocks:
            for line in block.get('lines',[]):
                value=''.join(span['text'] for span in line['spans']).strip()
                if value and not re.fullmatch(r'Page \d+',value) and value!='Powered by AIT':lines.append((line['bbox'][1],line['bbox'][0],value,any('bold' in span.get('font','').lower() for span in line['spans'])))
        lines.sort(key=lambda entry:(round(entry[0]/3),entry[1]))
        rows=[]
        for y,x,value,bold in lines:
            if rows and abs(rows[-1][0]-y)<3:rows[-1]=(rows[-1][0],rows[-1][1]+' '+value,rows[-1][2] or bold)
            else:rows.append((y,value,bold))
        if not rows:notices.append('PDF page '+str(index+1)+' has no extractable text and is not searchable. Images and scanned content are not interpreted by OCR.')
        pages.append(rows)
    core_start=next((i for i,rows in enumerate(pages) if any(re.fullmatch(r'(?:AIT )?Residential Core Lease Agreement',row[1]) for row in rows)),None)
    signature_start=max((i for i,rows in enumerate(pages) if core_start is not None and i>=core_start+core_pages and any(row[1]=='SIGNATURES' for row in rows)),default=None)
    if section_pages:
        core=next((part for part in section_pages if part['source']=='core'),None);signature=next((part for part in section_pages if part['source']=='signature'),None)
        if core:core_start=core['start']-1;core_pages=core['end']-core['start']+1
        if signature:signature_start=signature['start']-1
    units=[];active=None
    labels=('Tenant Reference','Monthly rental','Security deposit','Commencement','Duration / expiry','Renewal notice','Property / address','Unit','Selected account','Bank / branch','Account holder','Account number','Contracting party','Place of signing','Date of signing')
    def begin(source,reference,text,page):
        nonlocal active
        active={'position_y':round(y,2),'source':source,'source_label':CLAUSE_SOURCES.get(source,'Full Lease'),'reference':reference,'page':page,'fragments':[]}
        units.append(active);append(text,page)
    def append(value,page):
        if active['fragments'] and active['fragments'][-1]['page']==page:active['fragments'][-1]['text']+='\n'+value
        else:active['fragments'].append({'page':page,'text':value})
    for index,rows in enumerate(pages):
        page=index+1
        source='core' if core_start is not None and core_start<=index<core_start+core_pages else 'signature' if signature_start is not None and index>=signature_start else 'special_conditions' if core_start is not None and index>=core_start+core_pages else 'certificate' if index>0 else 'full_lease'
        if section_pages:source=next((part['source'] if part['source']!='cover' else 'full_lease' for part in section_pages if part['start']<=page<=part['end']),source)
        if active and active['source']!=source:active=None
        for y,value,bold in rows:
            match=re.match(r'^(\d+\.\d+(?:\.\d+)*)\s+',value) if source=='core' else re.match(r'^(SC[-– ]\d{1,3})(?=\s|[—:.-]|$)',value,re.I) if source=='special_conditions' else None
            parent=re.match(r'^(\d+)\s+',value) if source=='core' and bold else None
            field=next((label for label in labels if value.casefold().startswith(label.casefold()) and (len(value)==len(label) or value[len(label)] in ': |')),None) if source in ('certificate','signature') else None
            if match or parent:begin(source,(match or parent)[1],value,page)
            elif field:begin(source,field,value,page)
            elif active and active['source']==source and (source=='core' or (source=='special_conditions' and active['reference'] and (not bold or active['fragments'][-1]['text'].strip()==active['reference'])) or (source in ('certificate','signature') and active['reference'] and not bold)):
                append(value,page)
            else:begin(source,'',value,page)
    for unit in units:
        unit['original']='\n'.join(fragment['text'] for fragment in unit['fragments'])
        unit['description']=unit['reference'] or 'PDF page '+str(unit['page'])+' — text excerpt'
        unit['source_hash']=sha256(unit['original'].encode()).hexdigest();unit['id']='pdf:'+sha256((str(unit['page'])+'|'+str(unit['position_y'])+'|'+unit['source']+'|'+unit['reference']+'|'+unit['original']).encode()).hexdigest()[:32];unit['provenance']='Text extracted from the current Full Lease PDF'
    document.close();return units,notices


def full_lease_search(prop,draft):
    from flask import jsonify
    query=request.args.get('q','').strip()
    if len(query)>200:abort(400,description='Search text must be no longer than 200 characters.')
    index,_=full_lease_index(prop,draft);digest=index['source_sha256'];units=index['entries'];notices=index['notices']
    needle=' '.join(query.casefold().split());pattern=re.compile(r'(?<![\w.])'+re.escape(needle)+r'(?![\w.])') if re.fullmatch(r'(?:sc[-– ]\d+|\d+(?:\.\d+)+)',needle) else None
    matches=[]
    for unit in units:
        text=' '.join(unit['original'].casefold().split())
        if needle and not (pattern.search(text) if pattern else needle in text):continue
        position=pattern.search(text).start() if pattern else text.find(needle) if needle else 0
        excerpt=' '.join(unit['original'].split());start=max(0,position-80)
        matches.append(dict(id=unit['id'],source=unit['source'],source_label=unit['source_label'],reference=unit['reference'],page=unit['page'],title=unit.get('title',unit['original'].splitlines()[0]),source_version=digest,search_text=unit['original'],excerpt=excerpt[start:start+350]))
    return jsonify(results=matches,notices=notices,source_sha256=digest,state=full_lease_selection_state(draft))


def full_lease_select(prop,draft):
    from flask import jsonify
    key,_,_=full_lease_identity(draft)
    index=None
    if request.form.get('selected')=='yes':index,_=full_lease_index(prop,draft,existing_only=True)
    items=deepcopy(draft['items']);registers=items.setdefault('_lease_history',{'documents':[],'emails':[]}).setdefault('full_lease_selections',{})
    saved=deepcopy(registers.get(key,{'selected':[]}));selected={entry['id']:entry for entry in saved['selected']}
    if request.form.get('register_version')!=str(draft['version']):abort(409,description='The saved selections changed. Search again; your saved selections are unchanged.')
    checked=request.form.get('selected');identifier=request.form.get('provision_id','')
    if checked=='yes':
        digest=index['source_sha256']
        if request.form.get('source_sha256')!=digest:abort(409,description='The lease headings changed. Reopen Select Clause before selecting.')
        entry=next((unit for unit in index['entries'] if unit['id']==identifier),None)
        if entry is None:abort(404)
        entry=deepcopy(entry);entry['pdf_sha256']=digest;entry['location_page']=entry.get('page');entry['id']=digest+':'+identifier
        selected={identifier:saved_entry for identifier,saved_entry in selected.items() if (saved_entry['source'],saved_entry['reference'])!=(entry['source'],entry['reference'])}
        selected[entry['id']]=entry
    elif checked=='no':selected.pop(identifier,None)
    else:abort(400)
    saved.update(schema_version=1,selected=list(selected.values()),updated_at=now(),updated_by=int(current_user.id));registers[key]=saved;update_locked(draft,items)
    return jsonify(state=full_lease_selection_state(locked(prop['id'],draft['id'])))


def addendum_heading(provisions):
    groups=[]
    for source,label in CLAUSE_SOURCES.items():
        references=list(dict.fromkeys(entry['reference'] or 'PDF page '+str(entry['page'])+' (text excerpt)' for entry in provisions if entry['source']==source))
        if references:groups.append(label+' — '+', '.join(references))
    return ('NOTICE REGARDING CHANGES TO CLAUSES:','; '.join(groups)) if groups else ('GENERAL ADDENDUM TO AGREEMENT OF LEASE','')


def addendum_subject(provisions):
    titles=list(dict.fromkeys((entry.get('title') or entry.get('original','').split('\n')[0] or entry.get('reference') or 'PDF page '+str(entry['page'])).strip() for entry in provisions))
    return ('; '.join(titles)[:300] if titles else 'GENERAL AMENDMENT')


def addendum_precedence():
    return 'In the event of any inconsistency between this Addendum and the Lease Agreement, this Addendum shall prevail to the extent of that inconsistency. All other terms and conditions of the Lease Agreement remain unchanged and in full force and effect.'


def next_addendum_number(draft,original_id):
    return 1+max((entry.get('addendum_number',entry['version']) for entry in ledger(draft)['documents']
        if entry['kind']=='addendum_draft' and entry.get('original_id')==original_id),default=0)


def addendum_content(prop,draft):
    key,reference,_=full_lease_identity(draft);items=draft['items'];state=full_lease_selection_state(draft)
    if state['selected']:
        from werkzeug.exceptions import HTTPException
        try:index,_=full_lease_index(prop,draft,existing_only=True)
        except HTTPException as error:raise ValueError(error.description) from None
        available={(entry['source'],entry['reference']):entry for entry in index['entries']}
        if any((entry['source'],entry['reference']) not in available for entry in state['selected']):raise ValueError('A selected reference is no longer available. Return to Select Clause; your entered amendment is retained.')
        state['selected']=[deepcopy(available[(entry['source'],entry['reference'])]) for entry in state['selected']]
    for entry in state['selected']:entry['source_status']='Current source'
    subject=addendum_subject(state['selected']);wording=request.form.get('wording','')
    if not wording.strip() or len(wording)>30000 or '\x00' in wording:raise ValueError('Enter amendment content (maximum 30000 characters).')
    return key,items or {},reference,subject,wording,state['selected']


def addendum_pdf(prop,reference,wording,number,subject='',provisions=(),items=None):
    """Use one amendment content area; selected references never rewrite the lease."""
    from xml.sax.saxutils import escape
    from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,PageBreak,KeepTogether
    from reportlab.lib.styles import getSampleStyleSheet
    from .lease_signing import rows,signature_sections
    output=BytesIO();styles=getSampleStyleSheet();heading,references=addendum_heading(provisions)
    story=[Paragraph('ADDENDUM '+str(number).zfill(2),styles['Title']),
        Paragraph(escape(heading),styles['Heading2'])]
    if references:story.append(Paragraph(escape(references),styles['BodyText']))
    story.extend([Paragraph('Original Lease Agreement: '+escape(reference),styles['BodyText']),
        Paragraph('Property: '+escape(str(prop['name']))+' (ID '+str(prop['id'])+')',styles['BodyText']),Spacer(1,16)])
    if subject:story.append(Paragraph('SUBJECT: '+escape(subject),styles['Heading2']))
    story.append(Paragraph('Amendment Content',styles['Heading2']))
    for line in wording.splitlines():story.append(Paragraph(escape(line) or '&#160;',styles['BodyText']))
    story.extend([Spacer(1,16),Paragraph(escape(addendum_precedence()),styles['BodyText']),PageBreak(),Paragraph('SIGNATURES',styles['Title']),Paragraph('Addendum '+str(number).zfill(2)+' to '+escape(reference),styles['BodyText'])])
    parties=signature_sections(rows(items or {}),(items or {}).get('_parties',{}).get('lessor',{}))
    for party in parties:
        section=[Spacer(1,14),Paragraph(escape(party['statement']),styles['Heading3']),Paragraph(escape(party['role'].upper()),styles['Heading2']),Paragraph(escape(party['heading']),styles['Heading3'])]
        if party.get('name') or party.get('identity_number'):section.append(Paragraph(escape(' | '.join(value for value in (party.get('name',''),party.get('identity_number','')) if value)),styles['BodyText']))
        for representative in party.get('representatives',[]):
            if representative.get('name'):section.append(Paragraph('Authorised representative: '+escape(representative['name']),styles['BodyText']))
        for signer in party['signers']:
            if signer.get('name'):section.append(Paragraph(escape(' | '.join(value for value in (signer.get('name',''),signer.get('identity_number','')) if value)),styles['BodyText']))
            if signer.get('additional_particulars'):section.append(Paragraph('Signing particulars: '+escape(signer['additional_particulars']),styles['BodyText']))
            section.extend([Paragraph(escape(party['signer_label'])+' - printed name: '+escape(signer.get('printed_name') or '__________________________________'),styles['BodyText']),Paragraph('Signature: __________________________________',styles['BodyText'])])
            if party['role']=='Lessor' or signer.get('capacity'):section.append(Paragraph('Capacity: '+escape(signer.get('capacity') or '__________________________________'),styles['BodyText']))
        for number in (1,2):section.extend([Spacer(1,8),Paragraph('Witness '+str(number)+' - name: __________________________________',styles['BodyText']),Paragraph('Witness '+str(number)+' - signature: _________________________________',styles['BodyText'])])
        story.append(KeepTogether(section))
    SimpleDocTemplate(output,rightMargin=48,leftMargin=48,topMargin=48,bottomMargin=48).build(story)
    return output.getvalue()


def addendum_history(draft,property_id):
    result=[]
    for entry in ledger(draft)['documents']:
        if entry.get('kind') not in ('addendum_draft','signed_addendum') or entry.get('state')!='available':continue
        entry=deepcopy(entry)
        if entry.get('filename','').lower().endswith('.pdf'):
            token=signer().dumps(dict(kind='document',draft_id=str(draft['id']),property_id=property_id,
                version=draft['version'],sha256=entry['sha256'],nonce=str(uuid.uuid4()),document_id=entry['id']))
            entry['email_url']=url_for('billing_bp.lease_document_email',property_id=property_id,draft_id=draft['id'],document_token=token)
        result.append(entry)
    return result


@billing_bp.route('/billing/leases/property/<int:property_id>/addendum',methods=['GET','POST'],endpoint='lease_addendum')
@snapshot.endpoint
def addendum(property_id):
    prop,draft=context(property_id,request.method=='POST');errors=[]
    if (request.method=='GET' and request.args.get('action')=='clause_index') or (request.method=='POST' and request.form.get('action')=='retry_clause_index'):
        from flask import jsonify
        from werkzeug.exceptions import HTTPException
        try:
            index,_=full_lease_index(prop,draft)
            return jsonify(status='ready',results=index['entries'],notices=index['notices'],source_sha256=index['source_sha256'],state=full_lease_selection_state(draft))
        except HTTPException as error:return jsonify(error=error.description,retryable=True),error.code
    if request.method=='GET' and request.args.get('action')=='search_lease':
        from flask import jsonify
        from werkzeug.exceptions import HTTPException
        return jsonify(error='Live PDF search has been retired. Reopen Select Clause to load the persistent Clause Index and filter it in your browser.'),410
    if request.method=='GET' and request.args.get('action')=='lease_pdf':
        payload,digest=full_lease_source(prop,draft)
        if request.args.get('source_sha256') and request.args['source_sha256']!=digest:abort(409,description='The Full Lease changed. Search again to load its current pages.')
        response=send_file(BytesIO(payload),mimetype='application/pdf',as_attachment=False,download_name='Full-Lease.pdf',max_age=0)
        response.headers['Cache-Control']='private, no-store';response.headers['X-Content-Type-Options']='nosniff';return response
    if request.method=='GET' and request.args.get('workspace')=='selection':
        from werkzeug.exceptions import HTTPException
        heading_index=None;index_error=None
        try:heading_index,_=full_lease_index(prop,draft)
        except HTTPException as error:index_error=error.description
        return render_pm_template('program_billing/lease_addendum_select_pdf.html',title='Select Clause',property=prop,lease_draft=draft,clause_register=full_lease_selection_state(draft),heading_index=heading_index,index_error=index_error,unavailable=None)
    if request.method=='POST' and request.form.get('action')=='select_lease_pdf':
        from flask import jsonify
        from werkzeug.exceptions import HTTPException
        try:return full_lease_select(prop,draft)
        except HTTPException as error:db.session.rollback();return jsonify(error=error.description),error.code
    if request.method=='POST' and request.form.get('action') in ('addendum_clause_selection','addendum_manual_clause'):
        from flask import jsonify
        from werkzeug.exceptions import HTTPException
        try:return clause_register_save(prop,draft)
        except HTTPException as error:
            db.session.rollback();return jsonify(error=error.description),error.code
    if request.method=='POST' and request.form.get('action')=='addendum_preview':
        from flask import jsonify
        try:
            key,items,reference,subject,wording,provisions=addendum_content(prop,draft)
            payload=addendum_pdf(prop,reference,wording,next_addendum_number(draft,None if key=='draft' else key),subject,provisions,items)
        except ValueError as error:return jsonify(error=str(error)),400
        response=send_file(BytesIO(payload),mimetype='application/pdf',as_attachment=False,download_name='Addendum-preview.pdf',max_age=0)
        response.headers['Cache-Control']='private, no-store';response.headers['X-Content-Type-Options']='nosniff'
        return response
    arrangement_wording=''
    if request.method=='GET' and request.args.get('utility_arrangement_id'):
        from app.program_billing.lease_property_payments import utility_record
        arrangement=utility_record(property_id,request.args['utility_arrangement_id'])
        arrangement_wording=arrangement['details']['utility_allocation_method']
        if arrangement['details'].get('utility_references'):arrangement_wording+='\n'+arrangement['details']['utility_references']
    if request.method=='POST' and request.form.get('action') in ('upload_pdf','create_pdf'):
        from app.program_billing import lease_storage as storage
        request.max_content_length=storage.MAX_BYTES+1024*1024
        try:
            original_id=request.form.get('original_id','')
            original=None
            if request.form.get('selection_mode')=='full_pdf':
                _,_,original=full_lease_identity(draft)
            elif original_id:
                original_row,original,_=document(draft,original_id)
                if original['kind']!='signed_original':raise ValueError('Select the original signed lease to which this Addendum relates.')
            elif originals(draft):raise ValueError('Select the original signed lease to which this Addendum relates.')
            reference=original.get('lease_reference','') if original else 'Lease draft '+str(draft['id'])
            metadata={'original_id':original['id'] if original else None,'lease_reference':reference,
                'original_sha256':original['sha256'] if original else None,'legal_status':'not-verified'}
            if request.form['action']=='upload_pdf':
                upload=request.files.get('addendum_pdf')
                if not upload:raise ValueError('Select an Addendum PDF.')
                filename,extension,mime,payload,_=storage.validate_upload(upload)
                if extension!='.pdf':raise ValueError('Select a valid PDF.')
                entry=store(draft,'addendum_draft',filename,mime,payload,metadata,continuation=True)
            else:
                key,source_items,reference,subject,wording,provisions=addendum_content(prop,draft)
                metadata.update(wording=wording,subject=subject,selected_provisions=deepcopy(provisions))
                entry=store(draft,'addendum_draft','Addendum.pdf','application/pdf',None,metadata,continuation=True,
                    payload_builder=lambda number:addendum_pdf(prop,reference,wording,number,subject,provisions,source_items))
            flash('Addendum PDF saved. Open it below. The original lease is unchanged.','success')
            return redirect(url_for('billing_bp.lease_addendum',property_id=property_id,draft_id=draft['id'],workspace='view',saved_addendum=entry['id'],clause_lease=request.form.get('original_id') or request.form.get('clause_lease')))
        except ValueError as error:errors.append(str(error))
        draft=locked(property_id,draft['id'])
    elif request.method=='POST':
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
    register=full_lease_selection_state(draft)
    return render_pm_template('program_billing/lease_addendum.html',title='Addendum',property=prop,lease_draft=draft,
        clause_register=register,auto_subject=addendum_subject(full_lease_selection_state(draft)['selected']),precedence=addendum_precedence(),workspace_storage_key='lease-addendum:'+str(current_user.id)+':'+str(prop['id'])+':'+str(draft['id'])+':'+register['lease_key'],next_number=next_addendum_number(draft,None if register['lease_key']=='draft' else register['lease_key']),originals=originals(draft),addendums=addendum_history(draft,property_id),arrangement_wording=arrangement_wording,errors=errors,unavailable=None),400 if errors else 200


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
        return send_file(BytesIO(payload),mimetype=row['mime_type'],as_attachment=meta.get('kind') not in ('addendum_draft','signed_addendum'),download_name=row['filename'],max_age=0)
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
    if request.endpoint=='billing_bp.lease_addendum' and (request.args.get('action') in ('select_lease_pdf','clause_index','retry_clause_index') or (request.method=='POST' and request.form.get('action') in ('select_lease_pdf','retry_clause_index'))) and not response.is_json:
        from flask import jsonify
        code=response.status_code
        if 300<=code<400:code=401
        elif code<400:code=500
        messages={400:'The request or security token is invalid or expired. Reopen Select Clause before retrying.',401:'Your session has expired. Sign in again before retrying.',403:'You are not authorised to save selections for this lease.',404:'The selected property, draft or clause is unavailable.',409:'The lease changed. Reopen Select Clause before retrying.',413:'The selection request is too large.',503:'The lease database or document service is unavailable. Please retry.'}
        response=current_app.make_response((jsonify(error=messages.get(code,'The selection could not be saved because of a server error. Please retry.')),code))
    if request.endpoint in {'billing_bp.lease_history','billing_bp.lease_signed_copy','billing_bp.lease_addendum','billing_bp.lease_history_document',
        'billing_bp.lease_history_download','billing_bp.lease_document_email'}:
        response.headers['Cache-Control']='private, no-store';response.headers['X-Content-Type-Options']='nosniff'
        response.headers['Referrer-Policy']='same-origin'
    return response
