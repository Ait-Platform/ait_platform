"""Immutable, private Almond map attachments; no Core or clause mutation."""
from copy import deepcopy
from datetime import datetime,timezone
from hashlib import sha256
from io import BytesIO
from pathlib import PurePosixPath
import json,uuid,warnings
from flask import abort,jsonify,request,url_for
from flask_login import current_user
from sqlalchemy import text
from werkzeug.utils import secure_filename
from app.extensions import db
from . import lease_snapshot as snapshot,lease_storage as storage,lease_r2 as r2

IMAGES={'.png':'PNG','.jpg':'JPEG','.jpeg':'JPEG','.webp':'WEBP','.gif':'GIF','.tif':'TIFF','.tiff':'TIFF','.bmp':'BMP'}
MIMES={'.png':'image/png','.jpg':'image/jpeg','.jpeg':'image/jpeg','.webp':'image/webp','.gif':'image/gif','.tif':'image/tiff','.tiff':'image/tiff','.bmp':'image/bmp','.pdf':'application/pdf'}


def validate(file):
    name=secure_filename(file.filename or '');extension=PurePosixPath(name).suffix.lower()
    if not name or len(name)>200 or extension not in MIMES:raise ValueError('Select a PDF, PNG, JPEG, WebP, GIF, TIFF or BMP map.')
    payload=file.stream.read(storage.MAX_BYTES+1)
    if not payload or len(payload)>storage.MAX_BYTES:raise ValueError('Map files must be nonempty and no larger than 10 MB.')
    if extension=='.pdf':
        from pypdf import PdfReader
        try:
            reader=PdfReader(BytesIO(payload),strict=True)
            if reader.is_encrypted or not reader.pages:raise ValueError()
            for page in reader.pages:page.get_contents()
            if reader.trailer['/Root'].get('/OpenAction') or reader.trailer['/Root'].get('/AA'):raise ValueError()
            if reader.trailer['/Root'].get('/Names',{}).get('/JavaScript'):raise ValueError()
        except Exception:raise ValueError('Select a valid, unencrypted map PDF without automatic actions.') from None
    else:
        from PIL import Image
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('error',Image.DecompressionBombWarning)
                image=Image.open(BytesIO(payload))
                if image.format!=IMAGES[extension] or image.width*image.height>40000000 or getattr(image,'n_frames',1)!=1:raise ValueError()
                image.verify()
        except Exception:raise ValueError('Select a valid single-page map image no larger than 40 megapixels.') from None
    return name,MIMES[extension],payload,sha256(payload).hexdigest(),extension


def upload(prop,draft):
    from .lease_sc_import import require_editable
    if prop['id']!=26:abort(404)
    require_editable(draft)
    if str(draft['version'])!=request.form.get('version'):abort(409,description='The draft changed. Reload before uploading a map.')
    file=request.files.get('map')
    if not file:abort(400,description='Choose a map file.')
    try:name,mime,payload,digest,extension=validate(file)
    except ValueError as error:abort(400,description=str(error))
    snapshot.authorised_property(prop['id'],True);locked=snapshot.draft_row(prop['id'],draft['id'],True);require_editable(locked)
    if locked['version']!=draft['version']:abort(409,description='The draft changed. Reload before uploading a map.')
    registration=uuid.uuid5(uuid.NAMESPACE_URL,'ait-map:'+str(current_user.id)+':'+str(draft['id'])+':'+str(draft['version'])+':'+digest)
    existing=db.session.execute(text('SELECT id,version FROM bil_lease_draft WHERE property_id=:p AND manager_id=:u AND registration_token=:r'),{'p':prop['id'],'u':current_user.id,'r':registration}).mappings().first()
    if existing:return jsonify(saved=True,url=url_for('billing_bp.lease_history',property_id=prop['id'],draft_id=existing['id']),draft_id=str(existing['id']),version=existing['version'])
    identifier=uuid.uuid4();document=uuid.uuid4();key=storage.key_for(prop['id'],identifier,document,'.pdf').removesuffix('.pdf')+extension
    try:r2.upload_bytes_to_r2(key,payload,bucket=storage.destination(),content_type=mime)
    except Exception:db.session.rollback();abort(503,description='Private map storage was not confirmed. No new draft was saved.')
    items=deepcopy(locked['items']);now=datetime.now(timezone.utc).isoformat()
    maps=list(items.get('_map_annexures',[]));maps.append({'id':str(document),'draft_id':str(identifier),'filename':name,'mime_type':mime,'sha256':digest,'size_bytes':len(payload),'storage_key':key,'label':'Map Annexure'})
    items['_map_annexures']=maps;previous=items.pop('_lease_history',{'documents':[],'emails':[]})
    items['_map_source']={'draft_id':str(draft['id']),'version':draft['version'],'previous_history':previous}
    items['_lease_history']={'documents':[{'id':str(document),'kind':'map_annexure','version':1,'state':'available','filename':name,'sha256':digest,'uploaded_by':int(current_user.id),'uploaded_at':now}],'emails':[]}
    try:
        db.session.execute(text("""INSERT INTO bil_lease_draft(id,property_id,manager_id,registration_token,status,source_version,items,version)
            VALUES (:id,:p,:u,:registration,'draft',:source,CAST(:items AS jsonb),:version)"""),{'id':identifier,'p':prop['id'],'u':current_user.id,'registration':registration,'source':locked['source_version'],'items':json.dumps(items),'version':draft['version']+1})
        db.session.execute(text("""INSERT INTO bil_lease_document(id,draft_id,property_id,manager_id,item_key,storage_key,filename,mime_type,size_bytes,sha256,state,verified_at)
            VALUES (:id,:draft,:p,:u,'map-annexure',:key,:name,:mime,:size,:digest,'available',now())"""),{'id':document,'draft':identifier,'p':prop['id'],'u':current_user.id,'key':key,'name':name,'mime':mime,'size':len(payload),'digest':digest})
        db.session.commit()
    except Exception:db.session.rollback();abort(503,description='The new map draft was not confirmed. The original draft is unchanged.')
    return jsonify(saved=True,draft_id=str(identifier),version=draft['version']+1,url=url_for('billing_bp.lease_history',property_id=prop['id'],draft_id=identifier))


def source(context,entry):
    prop=context['property'];snapshot.authorised_property(prop['id'])
    row=db.session.execute(text("""SELECT * FROM bil_lease_document WHERE id=:id AND draft_id=:draft AND property_id=:p
        AND manager_id=:u AND item_key='map-annexure' AND state='available' AND verified_at IS NOT NULL"""),{'id':uuid.UUID(entry['id']),'draft':uuid.UUID(entry['draft_id']),'p':prop['id'],'u':current_user.id}).mappings().first()
    if row is None or row['sha256']!=entry['sha256']:raise ValueError('Map Annexure metadata is unavailable.')
    ext=PurePosixPath(row['filename']).suffix.lower()
    expected=storage.key_for(prop['id'],row['draft_id'],row['id'],'.pdf').removesuffix('.pdf')+ext
    if row['storage_key']!=expected or ext not in MIMES:raise ValueError('Map Annexure storage scope is invalid.')
    payload=r2.read_file_from_r2(expected,bucket=storage.destination())
    if len(payload)!=row['size_bytes'] or sha256(payload).hexdigest()!=row['sha256']:raise ValueError('Map Annexure integrity failed.')
    return payload,row['mime_type'],row['filename']


def compiled(context):
    from pypdf import PdfReader,PdfWriter
    from reportlab.pdfgen import canvas
    from reportlab.lib.utils import ImageReader
    from PIL import Image,ImageOps
    for entry in context['lease_draft']['items'].get('_map_annexures',[]):
        payload,mime,name=source(context,entry);output=BytesIO();page=canvas.Canvas(output,pagesize=(595.28,841.89),invariant=1)
        page.setFont('Helvetica-Bold',16);page.drawString(42,795,'Map Annexure');page.setFont('Helvetica',11);page.drawString(42,775,context.get('premises_name') or context['property'].get('name',''))
        if mime!='application/pdf':
            image=ImageOps.exif_transpose(Image.open(BytesIO(payload)));image.seek(0)
            if image.mode!='RGB':
                converted=Image.new('RGB',image.size,'white')
                if 'A' in image.getbands() or 'transparency' in image.info:
                    image=image.convert('RGBA');converted.paste(image,mask=image.getchannel('A'))
                else:converted.paste(image.convert('RGB'))
                image=converted
            scale=min(511/image.width,700/image.height);w=image.width*scale;h=image.height*scale
            page.drawImage(ImageReader(image),42+(511-w)/2,55+(700-h)/2,width=w,height=h)
        page.showPage();page.save();writer=PdfWriter();writer.append(PdfReader(BytesIO(output.getvalue())))
        if mime=='application/pdf':writer.append(PdfReader(BytesIO(payload)))
        result=BytesIO();writer.write(result);yield result.getvalue()
