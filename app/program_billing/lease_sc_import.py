"""Strict Word comparison and explicitly confirmed, immutable draft continuation."""
from copy import deepcopy
from datetime import datetime,timezone
from hashlib import sha256
from io import BytesIO
import json,re,uuid
from zipfile import ZipFile
from lxml import etree as ET
from flask import abort,current_app,jsonify,request,url_for
from flask_login import current_user
from itsdangerous import URLSafeTimedSerializer,BadSignature
from sqlalchemy import text
from app.extensions import db
from . import lease_snapshot as snapshot,lease_storage as storage
from .lease_sc_export import W


def signer():return URLSafeTimedSerializer(current_app.secret_key,salt='lease-sc-word-import')


def require_editable(draft):
    items=draft['items']
    if any(e.get('kind','').startswith('signed_') for e in items.get('_lease_history',{}).get('documents',[])) or any(g.get('signature_or_issuance_approved') is True for g in items.get('_generations',[])+items.get('_ait_standard',{}).get('generations',[])):
        abort(409,description='Signed or approved drafts cannot be changed by Word import.')


def editable(draft):
    require_editable(draft)
    items=draft['items']
    from .lease_supplementary_conditions import legacy_entries
    if legacy_entries(items):abort(409,description='This draft contains legacy condition structures. They cannot be safely matched by SC number for import.')
    saved=items.get('_supplementary_conditions',{})
    if not isinstance(saved,dict):abort(409,description='Saved Special Conditions use an unsupported structure.')
    clauses=saved.get('clauses',[])
    if not clauses or not isinstance(clauses,list):abort(409,description='Save numbered Special Conditions before importing a replacement.')
    if any(not isinstance(c,dict) or not isinstance(c.get('heading'),str) or not isinstance(c.get('text'),str) for c in clauses):abort(409,description='Saved clause particulars are incomplete.')
    numbers=[str(c.get('number','SC-%02d'%i)) for i,c in enumerate(clauses,1)]
    if len(set(numbers))!=len(numbers) or any(not re.fullmatch(r'SC-\d+',n) for n in numbers):abort(409,description='Saved clause numbering is ambiguous.')
    return clauses,numbers


def paragraphs(payload):
    with ZipFile(BytesIO(payload)) as archive:
        if len(set(archive.namelist()))!=len(archive.namelist()):raise ValueError('Duplicate DOCX parts.')
        for name in archive.namelist():
            if name.endswith('.rels'):
                rel=ET.fromstring(archive.read(name),parser=ET.XMLParser(resolve_entities=False,no_network=True))
                if any(r.get('TargetMode','').casefold()=='external' or re.match(r'^[a-zA-Z][a-zA-Z0-9+.-]*:|^//',r.get('Target','')) for r in rel):raise ValueError('External links are not supported.')
            if re.fullmatch(r'word/(header|footer)\d+\.xml',name):
                part=ET.fromstring(archive.read(name),parser=ET.XMLParser(resolve_entities=False,no_network=True))
                if any((t.text or '') for t in part.iter('{'+W+'}t')):raise ValueError('Header or footer wording is not supported.')
            if re.search(r'word/(media|embeddings)/|word/(footnotes|endnotes|comments)\.xml',name):raise ValueError('Embedded or annotated content is not supported.')
        root=ET.fromstring(archive.read('word/document.xml'),parser=ET.XMLParser(resolve_entities=False,no_network=True))
    body=root.find('{'+W+'}body');result=[]
    if body is None:raise ValueError('Missing document body.')
    def read_paragraph(p):
        if p.find('.//{'+W+'}numPr') is not None:raise ValueError('Use explicit SC and internal numbers; automatic list numbering is ambiguous.')
        parts=[]
        for run in p:
            if run.tag in ('{'+W+'}pPr','{'+W+'}proofErr','{'+W+'}bookmarkStart','{'+W+'}bookmarkEnd'):continue
            if run.tag!='{'+W+'}r':raise ValueError('Resolve tracked changes, fields and unsupported content before importing.')
            for node in run:
                if node.tag in ('{'+W+'}rPr','{'+W+'}lastRenderedPageBreak'):continue
                if node.tag=='{'+W+'}t':parts.append(node.text or '')
                elif node.tag=='{'+W+'}tab':parts.append('\t')
                elif node.tag in ('{'+W+'}br','{'+W+'}cr') and node.get('{'+W+'}type','textWrapping')=='textWrapping':parts.append('\n')
                else:raise ValueError('Unsupported Word run structure.')
        return ''.join(parts)
    for node in body:
        if node.tag=='{'+W+'}sectPr':continue
        if node.tag=='{'+W+'}p':result.append({'kind':'paragraph','text':read_paragraph(node)})
        elif node.tag=='{'+W+'}tbl':
            rows=[]
            for row in node:
                if row.tag in ('{'+W+'}tblPr','{'+W+'}tblGrid'):continue
                if row.tag!='{'+W+'}tr':raise ValueError('Unsupported table row structure.')
                cells=[]
                for cell in row:
                    if cell.tag=='{'+W+'}trPr':continue
                    if cell.tag!='{'+W+'}tc':raise ValueError('Unsupported table cell structure.')
                    if cell.find('.//{'+W+'}gridSpan') is not None or cell.find('.//{'+W+'}vMerge') is not None:raise ValueError('Merged table cells are ambiguous; use separate rectangular cells.')
                    values=[]
                    for part in cell:
                        if part.tag=='{'+W+'}tcPr':continue
                        if part.tag!='{'+W+'}p':raise ValueError('Nested tables and content controls are unsupported.')
                        values.append(read_paragraph(part))
                    cells.append('\n'.join(values))
                rows.append(cells)
            if not rows or not rows[0] or any(len(row)!=len(rows[0]) for row in rows):raise ValueError('The Word table must contain complete rectangular rows.')
            result.append({'kind':'table','rows':rows})
        else:raise ValueError('Use Word paragraphs, headings and rectangular tables only.')
    return result


def proposed(payload,clauses,numbers):
    from .lease_sc_blocks import plain_text
    found=[];active=None;content=[];await_heading=False
    def finish():
        if active is not None:
            if not active[1].strip():raise ValueError(active[0]+': clause heading is missing.')
            if not plain_text(content).strip():raise ValueError(active[0]+': clause wording is missing.')
            found.append((active[0],active[1],deepcopy(content)))
    for block in paragraphs(payload):
        paragraph=block.get('text','') if block['kind']=='paragraph' else ''
        match=re.fullmatch(r'(SC-\d+)(?: [—–-] (.*))?',paragraph) or re.fullmatch(r'\s*(SC-\d+)(?:(?:\s*[—–:]\s*|\s+-\s+|\s+)(.+))?\s*',paragraph)
        if match:
            finish();active=[match[1],match[2] or ''];content=[];await_heading=not active[1]
        elif block['kind']=='paragraph' and paragraph.lstrip().startswith('SC-'):raise ValueError('Ambiguous clause number or heading: '+paragraph[:100])
        elif active is not None:
            if await_heading and block['kind']=='paragraph' and paragraph.strip():active[1]=paragraph;await_heading=False
            else:content.append(block)
        elif block['kind']=='table':raise ValueError('A table must belong to an identified SC clause.')
        elif paragraph and paragraph!='Special Conditions' and not paragraph.startswith(('Property: ','Tenant: ','Lease draft ID: ','Draft version: ','Export date: ','Saved Special Conditions selection: ')):
            raise ValueError('Unnumbered wording appears before the first SC clause.')
    finish()
    actual=[r[0] for r in found]
    duplicate=next((n for n in actual if actual.count(n)>1),None)
    if duplicate:raise ValueError('Duplicate clause number: '+duplicate)
    missing=[n for n in numbers if n not in actual]
    if missing:raise ValueError('Missing clause numbers: '+', '.join(missing))
    if actual!=numbers:raise ValueError('SC numbers must match the saved clause order exactly; unexpected or reordered numbers were found.')
    if len({(heading,plain_text(blocks)) for _,heading,blocks in found})!=len(found):raise ValueError('Duplicate clauses are not accepted.')
    result=deepcopy(clauses)
    for target,(_,heading,blocks) in zip(result,found):
        if heading!=target.get('heading'):target['heading_type']='Custom'
        target.update(heading=heading,text=plain_text(blocks),blocks=blocks)
    return result


def selection(draft,digest,clauses):
    return {'user':str(current_user.id),'property':draft['property_id'],'draft':str(draft['id']),'version':draft['version'],'sha256':digest,'clauses':sha256(json.dumps(clauses,ensure_ascii=False,sort_keys=True).encode()).hexdigest()}


def handle(prop,draft):
    clauses,numbers=editable(draft)
    file=request.files.get('source')
    if not file:abort(400,description='Select a DOCX source document.')
    try:
        filename,mime,payload,digest=storage.validate_master(file)
        changes=proposed(payload,clauses,numbers)
    except ValueError as error:abort(400,description=str(error))
    chosen=selection(draft,digest,changes)
    if request.form['action']=='sc_import_preview':
        return jsonify(token=signer().dumps(chosen),version=draft['version'],comparison=[{'number':n,'existing_heading':old['heading'],'existing_text':old['text'],'proposed_heading':new['heading'],'proposed_text':new['text'],'existing_blocks':old.get('blocks'),'proposed_blocks':new.get('blocks')} for n,old,new in zip(numbers,clauses,changes)])
    if request.form.get('confirmed')!='yes':abort(400,description='Explicit confirmation is required.')
    try:verified=signer().loads(request.form.get('token',''),max_age=900)
    except BadSignature:abort(409,description='Import comparison expired or is invalid. Compare the document again.')
    if verified!=chosen:abort(409,description='The source document or saved draft changed. Compare again before confirming.')
    snapshot.authorised_property(prop['id'],True)
    locked=snapshot.draft_row(prop['id'],draft['id'],True)
    editable(locked)
    if locked['version']!=draft['version']:abort(409,description='The draft changed. Compare again before confirming.')
    registration=uuid.uuid5(uuid.NAMESPACE_URL,'ait-sc-import:'+json.dumps(chosen,sort_keys=True))
    existing=db.session.execute(text('SELECT id,version FROM bil_lease_draft WHERE property_id=:p AND manager_id=:u AND registration_token=:r'),{'p':prop['id'],'u':current_user.id,'r':registration}).mappings().first()
    if existing:return jsonify(saved=True,draft_id=str(existing['id']),version=existing['version'],url=url_for('billing_bp.lease_history',property_id=prop['id'],draft_id=existing['id']))
    new_id=uuid.uuid4();document_id=uuid.uuid4();key=storage.key_for(prop['id'],new_id,document_id,'.docx')
    # Upload and verified read-back use the existing private adapter, never a public URL.
    try:storage.upload(key,payload,mime)
    except Exception:
        db.session.rollback();abort(503,description='Private source storage was not confirmed. No new draft was saved.')
    imported_at=datetime.now(timezone.utc).isoformat()
    items=deepcopy(locked['items']);items['_supplementary_conditions']['clauses']=changes
    history=items.pop('_lease_history',{'documents':[],'emails':[]})
    items['_sc_import']={'source_draft_id':str(draft['id']),'source_version':draft['version'],'source_sha256':digest,'document_id':str(document_id),'imported_at':imported_at,'imported_by':int(current_user.id),'previous_history':history}
    items['_lease_history']={'documents':[{'id':str(document_id),'kind':'special_conditions_import','version':1,'state':'available','filename':filename,'sha256':digest,'uploaded_by':int(current_user.id),'uploaded_at':imported_at,'signature_status':'not-verified'}],'emails':[]}
    try:
        db.session.execute(text("""INSERT INTO bil_lease_draft (id,property_id,manager_id,registration_token,status,source_version,items,version)
            VALUES (:id,:p,:u,:registration,'draft',:source,CAST(:items AS jsonb),:version)"""),{'id':new_id,'p':prop['id'],'u':current_user.id,'registration':registration,'source':locked['source_version'],'items':json.dumps(items),'version':draft['version']+1})
        db.session.execute(text("""INSERT INTO bil_lease_document (id,draft_id,property_id,manager_id,item_key,storage_key,filename,mime_type,size_bytes,sha256,state,verified_at)
            VALUES (:id,:draft,:p,:u,'history-special_conditions_import',:key,:filename,:mime,:size,:digest,'available',now())"""),{'id':document_id,'draft':new_id,'p':prop['id'],'u':current_user.id,'key':key,'filename':filename,'mime':mime,'size':len(payload),'digest':digest})
        db.session.commit()
    except Exception:
        db.session.rollback();current_app.logger.warning('SC import publication failed property=%s source_draft=%s private_document=%s; private source retained for reconciliation.',prop['id'],draft['id'],document_id)
        abort(503,description='The new draft was not confirmed. The original draft remains unchanged.')
    return jsonify(saved=True,draft_id=str(new_id),version=draft['version']+1,url=url_for('billing_bp.lease_history',property_id=prop['id'],draft_id=new_id))
