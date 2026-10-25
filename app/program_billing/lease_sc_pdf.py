"""Previewed and explicitly confirmed SC PDFs with immutable private filing history."""
from copy import deepcopy
from datetime import datetime,timezone
from hashlib import sha256
from io import BytesIO
import json,uuid
from flask import abort,current_app,jsonify,request,send_file,url_for
from flask_login import current_user
from itsdangerous import URLSafeTimedSerializer,BadSignature
from sqlalchemy import text
from werkzeug.exceptions import HTTPException
from app.extensions import db
from . import lease_snapshot as snapshot,lease_storage as storage


def signer():return URLSafeTimedSerializer(current_app.secret_key,salt='lease-sc-filed-pdf')


def entries(draft):return draft['items'].get('_sc_pdf_versions',[])


def current(draft):
    identifier=draft['items'].get('_sc_pdf_current')
    if not identifier:return None
    entry=next((entry for entry in entries(draft) if entry['id']==identifier),None)
    if entry is None:raise ValueError('Current SC PDF is not present in the Filing Cabinet.')
    return entry


def retrieve(prop,draft,identifier=None):
    snapshot.authorised_property(prop['id'])
    entry=next((entry for entry in entries(draft) if entry['id']==str(identifier)),None) if identifier else current(draft)
    if entry is None:abort(404,description='The selected SC PDF is not filed against this draft.')
    row=db.session.execute(text("""SELECT * FROM bil_lease_document WHERE id=:id AND draft_id=:draft
        AND property_id=:p AND manager_id=:u AND item_key='special-conditions-pdf' AND state='available' AND verified_at IS NOT NULL"""),{'id':uuid.UUID(entry['id']),'draft':uuid.UUID(entry['draft_id']),'p':prop['id'],'u':current_user.id}).mappings().first()
    if row is None or row['sha256']!=entry['sha256']:abort(409,description='The filed SC PDF metadata could not be verified.')
    try:return storage.retrieve(row),entry
    except Exception:abort(503,description='The private SC PDF could not be retrieved and verified. No generated substitute was used.')


def handle(prop,draft):
    from .lease_sc_import import require_editable
    require_editable(draft)
    action=request.form['action']
    if action=='record_signature_review':
        if request.form.get('review_complete')!='on':abort(400,description='Confirm that you have reviewed the complete Lease Agreement.')
        if request.form.get('version')!=str(draft['version']):abort(409,description='The draft changed. Review the current agreement before confirming.')
        from .lease_signature_review import record,requires_pdf
        if requires_pdf(draft['items']) and not current(draft):abort(409,description='Upload, preview and confirm the complete SC PDF before recording your final review.')
        from .lease_workflow import save_items
        items=deepcopy(draft['items']);record(items,current_user.id);save_items(draft,items)
        return jsonify(saved=True,url=url_for('billing_bp.lease_history',property_id=prop['id'],draft_id=draft['id']))
    upload=request.files.get('source')
    if not upload:abort(400,description='Select the Special Conditions PDF.')
    try:
        filename,ext,mime,payload,digest=storage.validate_upload(upload)
        if ext!='.pdf':raise ValueError('Upload a PDF saved from your final Word document.')
        from pypdf import PdfReader
        reader=PdfReader(BytesIO(payload),strict=True)
        for page in reader.pages:page.get_contents()
        root=reader.trailer['/Root']
        pdf_action=root.get('/OpenAction')
        if pdf_action and hasattr(pdf_action,'get') and pdf_action.get('/S') not in (None,'/GoTo'):raise ValueError('Active PDF actions are not supported.')
        if root.get('/AA'):raise ValueError('Automatic PDF actions are not supported.')
        names=root.get('/Names')
        if names and names.get_object().get('/JavaScript'):raise ValueError('PDF JavaScript is not supported.')
    except Exception as error:abort(400,description=str(error) if isinstance(error,ValueError) else 'Select a valid unencrypted PDF.')
    chosen={'user':str(current_user.id),'property':prop['id'],'draft':str(draft['id']),'version':draft['version'],'sha256':digest}
    if action=='sc_pdf_preview':return jsonify(token=signer().dumps(chosen),pages=len(reader.pages),sha256=digest)
    if request.form.get('confirmed')!='yes':abort(400,description='Preview the PDF and explicitly confirm it before filing.')
    try:token=signer().loads(request.form.get('token',''),max_age=900)
    except BadSignature:abort(409,description='The PDF preview expired or is invalid. Preview it again.')
    if token!=chosen:abort(409,description='The PDF or lease draft changed. Preview the current file again.')
    snapshot.authorised_property(prop['id'],True);locked=snapshot.draft_row(prop['id'],draft['id'],True);require_editable(locked)
    if locked['version']!=draft['version']:abort(409,description='The draft changed. Preview again.')
    identifier=uuid.uuid4();key=storage.key_for(prop['id'],draft['id'],identifier,'.pdf')
    try:storage.upload(key,payload,mime)
    except Exception:db.session.rollback();abort(503,description='Private PDF storage was not confirmed. The current PDF is unchanged.')
    items=deepcopy(locked['items']);versions=list(items.get('_sc_pdf_versions',[]))
    entry={'id':str(identifier),'draft_id':str(draft['id']),'version':1+max((e['version'] for e in versions),default=0),'filename':filename,'sha256':digest,'uploaded_at':datetime.now(timezone.utc).isoformat(),'uploaded_by':int(current_user.id)}
    versions.append(entry);items['_sc_pdf_versions']=versions;items['_sc_pdf_current']=entry['id']
    if '_ait_standard' in items:items['_ait_standard']['status']='draft'
    try:
        db.session.execute(text("""INSERT INTO bil_lease_document(id,draft_id,property_id,manager_id,item_key,storage_key,filename,mime_type,size_bytes,sha256,state,verified_at)
            VALUES (:id,:draft,:p,:u,'special-conditions-pdf',:key,:name,:mime,:size,:digest,'available',now())"""),{'id':identifier,'draft':draft['id'],'p':prop['id'],'u':current_user.id,'key':key,'name':filename,'mime':mime,'size':len(payload),'digest':digest})
        updated=db.session.execute(text("""UPDATE bil_lease_draft SET items=CAST(:items AS jsonb),version=version+1,updated_at=now()
            WHERE id=:draft AND property_id=:p AND manager_id=:u AND version=:version RETURNING id"""),{'items':json.dumps(items),'draft':draft['id'],'p':prop['id'],'u':current_user.id,'version':draft['version']}).scalar_one_or_none()
        if updated is None:abort(409,description='The draft changed. No current PDF was replaced.')
        db.session.commit()
    except HTTPException:
        db.session.rollback();raise
    except Exception:
        db.session.rollback();current_app.logger.warning('SC PDF filing failed property=%s draft=%s document=%s; private source retained for reconciliation.',prop['id'],draft['id'],identifier)
        abort(503,description='Filing was not confirmed. The previous current PDF remains unchanged.')
    return jsonify(saved=True,url=url_for('billing_bp.lease_history',property_id=prop['id'],draft_id=draft['id']))


def converted_word(payload):
    """Editable text and extracted images; no legal or layout equivalence is claimed."""
    import fitz
    from zipfile import ZipFile,ZIP_DEFLATED
    from xml.etree import ElementTree as ET
    from .lease_sc_export import W
    doc=ET.Element('{'+W+'}document');body=ET.SubElement(doc,'{'+W+'}body');media=[]
    R='http://schemas.openxmlformats.org/officeDocument/2006/relationships'
    def paragraph(value):
        p=ET.SubElement(body,'{'+W+'}p')
        for index,line in enumerate(value.split('\n')):
            r=ET.SubElement(p,'{'+W+'}r')
            if index:ET.SubElement(r,'{'+W+'}br')
            ET.SubElement(r,'{'+W+'}t',{'{http://www.w3.org/XML/1998/namespace}space':'preserve'}).text=line
    def image(data,width,height):
        from PIL import Image
        converted=BytesIO();Image.open(BytesIO(data)).convert('RGB').save(converted,format='PNG');media.append(converted.getvalue());i=len(media)
        p=ET.SubElement(body,'{'+W+'}p');run=ET.SubElement(p,'{'+W+'}r')
        # Fixed aspect ratio, constrained to the printable page area.
        factor=min(460/width,640/height,1);cx=int(width*factor*12700);cy=int(height*factor*12700)
        drawing=f'''<w:drawing xmlns:w="{W}" xmlns:r="{R}" xmlns:wp="http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing" xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:pic="http://schemas.openxmlformats.org/drawingml/2006/picture"><wp:inline><wp:extent cx="{cx}" cy="{cy}"/><wp:docPr id="{i}" name="PDF image {i}"/><a:graphic><a:graphicData uri="http://schemas.openxmlformats.org/drawingml/2006/picture"><pic:pic><pic:nvPicPr><pic:cNvPr id="{i}" name="image{i}.png"/><pic:cNvPicPr/></pic:nvPicPr><pic:blipFill><a:blip r:embed="rId{i}"/><a:stretch><a:fillRect/></a:stretch></pic:blipFill><pic:spPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="{cx}" cy="{cy}"/></a:xfrm><a:prstGeom prst="rect"><a:avLst/></a:prstGeom></pic:spPr></pic:pic></a:graphicData></a:graphic></wp:inline></w:drawing>'''
        run.append(ET.fromstring(drawing))
    warning='Conversion warning: check against the official PDF. Layout, tables, numbering, vector maps, photographs and scanned text may not convert faithfully. Scanned pages remain images; OCR is not performed.'
    document=fitz.open(stream=payload,filetype='pdf')
    for page in document:
        blocks=page.get_text('dict',sort=True)['blocks'];has_text=False
        for block in blocks:
            if block['type']==0:
                value='\n'.join(''.join(span['text'] for span in line['spans']) for line in block['lines']);paragraph(value);has_text=has_text or bool(value.strip())
            elif block['type']==1:
                box=block.get('bbox',(0,0,block['width'],block['height']));image(block['image'],box[2]-box[0],box[3]-box[1])
        if not has_text and not any(b['type']==1 for b in blocks):image(page.get_pixmap().tobytes('png'),page.rect.width,page.rect.height)
    document.close();output=BytesIO()
    with ZipFile(output,'w',ZIP_DEFLATED) as archive:
        archive.writestr('[Content_Types].xml','<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Default Extension="png" ContentType="image/png"/><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/><Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/></Types>')
        archive.writestr('_rels/.rels',f'<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="{R}/officeDocument" Target="word/document.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/></Relationships>')
        core=ET.Element('{http://schemas.openxmlformats.org/package/2006/metadata/core-properties}coreProperties');ET.SubElement(core,'{http://purl.org/dc/elements/1.1/}description').text=warning
        archive.writestr('docProps/core.xml',ET.tostring(core,encoding='utf-8',xml_declaration=True))
        archive.writestr('word/document.xml',ET.tostring(doc,encoding='utf-8',xml_declaration=True))
        archive.writestr('word/_rels/document.xml.rels','<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'+''.join(f'<Relationship Id="rId{i}" Type="{R}/image" Target="media/image{i}.png"/>' for i in range(1,len(media)+1))+'</Relationships>')
        for i,data in enumerate(media,1):archive.writestr('word/media/image'+str(i)+'.png',data)
    return output.getvalue()


def file_response(prop,draft,identifier):
    payload,entry=retrieve(prop,draft,identifier)
    word=request.args.get('word')=='1'
    if word:payload=converted_word(payload)
    response=send_file(BytesIO(payload),mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document' if word else 'application/pdf',as_attachment=word or request.args.get('download')=='1',download_name='Special-Conditions-v'+str(entry['version'])+('.docx' if word else '.pdf'),conditional=False,etag=False,max_age=0)
    response.headers['Cache-Control']='private, no-store';response.headers['X-Content-Type-Options']='nosniff'
    response.headers['Content-Security-Policy']="frame-ancestors 'self'"
    response.headers['Referrer-Policy']='no-referrer'
    return response
