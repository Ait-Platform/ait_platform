"""Private property-scoped SOA presentation. Never writes ledger or METSOA data."""
import hashlib,json,uuid,warnings
from io import BytesIO
from pathlib import PurePosixPath
from flask import abort
from flask_login import current_user
from itsdangerous import BadSignature
from sqlalchemy import inspect,text
from PIL import Image
from . import lease_soa_posting as posting
from . import lease_storage as storage
MAX_BYTES=5*1024*1024


def authorise(property_id):
    row=posting.db.session.execute(text('SELECT id,name,address FROM bil_property WHERE id=:p AND manager_id=:m AND NOT is_archived FOR SHARE'),dict(p=property_id,m=current_user.id)).mappings().first()
    if row is None:abort(404)
    return dict(row)


def ready():
    catalog=inspect(posting.db.session.connection())
    return catalog.has_table('bil_pm_soa_branding')


def rows(property_id):
    authorise(property_id)
    if not ready():return []
    return [dict(r) for r in posting.db.session.execute(text('SELECT * FROM bil_pm_soa_branding WHERE property_id=:p AND manager_id=:m ORDER BY created_at,id'),dict(p=property_id,m=current_user.id)).mappings()]


def selected(property_id):return [row for row in rows(property_id) if row['selected']]


def form_key(property_id):return posting.token(property_id,0,'soa-branding')


def validate_image(file,kind):
    name=(file.filename or '').replace('\\','/').rsplit('/',1)[-1]
    extension=PurePosixPath(name).suffix.lower()
    if not name or len(name)>200 or extension not in ('.png','.jpg','.jpeg','.pdf') or (kind=='logo' and extension=='.pdf'):
        raise ValueError('Choose a PNG/JPEG logo, or a single-page PDF/PNG/JPEG letterhead.')
    payload=file.stream.read(MAX_BYTES+1)
    if not payload or len(payload)>MAX_BYTES:raise ValueError('Choose a nonempty file no larger than 5 MB.')
    try:
        if extension=='.pdf':
            import fitz
            if not payload.startswith(b'%PDF-'):raise ValueError()
            with fitz.open(stream=payload,filetype='pdf') as document:
                if document.needs_pass or document.page_count!=1:raise ValueError()
                page=document[0]
                if page.rect.width<=0 or page.rect.height<=0 or max(page.rect.width,page.rect.height)>3000:raise ValueError()
                image=Image.open(BytesIO(page.get_pixmap(matrix=fitz.Matrix(1.5,1.5),alpha=False).tobytes('png')))
            mime='application/pdf'
        else:
            with warnings.catch_warnings():
                warnings.simplefilter('error',Image.DecompressionBombWarning)
                image=Image.open(BytesIO(payload))
                if image.format not in ('PNG','JPEG') or image.width*image.height>20000000 or getattr(image,'n_frames',1)!=1:raise ValueError()
                image.load()
            mime='image/png' if image.format=='PNG' else 'image/jpeg'
        image.thumbnail((1800,1800));output=BytesIO();image.convert('RGBA').save(output,format='PNG')
    except Exception as exc:raise ValueError('The image or single-page PDF could not be safely read.') from exc
    return name,extension,mime,payload,output.getvalue()


def save(property_id,form,files):
    authorise(property_id)
    if not ready():abort(503,description='SOA Branding requires migration pm_soa_brand_001. No financial records were changed.')
    posting.db.session.execute(text('SELECT pg_advisory_xact_lock(1836020593,:p)'),dict(p=property_id))
    action=form.get('action')
    if action in ('select','clear'):
        kind=form.get('kind')
        if kind not in ('logo','letterhead'):raise ValueError('Choose a branding type.')
        chosen=None
        if action=='select':
            try:identifier=str(uuid.UUID(form.get('id','')))
            except ValueError:abort(400)
            chosen=next((r for r in rows(property_id) if str(r['id'])==identifier and r['kind']==kind),None)
            if chosen is None:abort(404)
        posting.db.session.execute(text('UPDATE bil_pm_soa_branding SET selected=false WHERE property_id=:p AND manager_id=:m AND kind=:kind'),dict(p=property_id,m=current_user.id,kind=kind))
        if chosen:posting.db.session.execute(text('UPDATE bil_pm_soa_branding SET selected=true WHERE id=:id'),dict(id=str(chosen['id'])))
        posting.db.session.commit();return 'Branding selection saved.'
    if action not in ('upload','create'):raise ValueError('Choose a branding action.')
    try:
        scope=posting.signer().loads(form.get('form_key',''))
        if not isinstance(scope,list) or len(scope)!=5 or scope[:4]!=[current_user.id,property_id,0,'soa-branding']:raise BadSignature('scope')
    except (BadSignature,TypeError):raise ValueError('This branding form is invalid. Open the hub again.')
    if posting.db.session.execute(text('SELECT id FROM bil_pm_soa_branding WHERE create_key=:key'),dict(key=scope[4])).first():
        posting.db.session.commit();return 'Branding was already saved.'
    kind=form.get('kind');title=form.get('title','').strip()
    if kind not in ('logo','letterhead') or not 1<=len(title)<=120:raise ValueError('Choose a type and a title of at most 120 characters.')
    identifier=str(uuid.uuid4());details={}
    if action=='create':
        if kind!='letterhead':raise ValueError('Simple creation is for letterheads.')
        details={key:form.get(key,'').strip() for key in ('business_name','address','contact')}
        if not details['business_name'] or any(len(value)>500 or '\x00' in value for value in details.values()):raise ValueError('Enter a business name; each field may contain at most 500 characters.')
        details['format']='text'
    else:
        file=files.get('branding_file')
        if file is None:raise ValueError('Select a branding file.')
        name,ext,mime,original,preview=validate_image(file,kind)
        prefix=f'private/pm-soa/manager-{current_user.id}/property-{property_id}/branding-{identifier}'
        storage.upload(prefix+'/original'+ext,original,mime)
        storage.upload(prefix+'/preview.png',preview,'image/png')
        details=dict(format='image',filename=name,original_key=prefix+'/original'+ext,preview_key=prefix+'/preview.png',sha256=hashlib.sha256(preview).hexdigest(),original_sha256=hashlib.sha256(original).hexdigest(),size=len(preview))
    posting.db.session.execute(text("INSERT INTO bil_pm_soa_branding(id,property_id,manager_id,kind,title,details,create_key) VALUES(:id,:p,:m,:kind,:title,CAST(:details AS jsonb),:key)"),dict(id=identifier,p=property_id,m=current_user.id,kind=kind,title=title,details=json.dumps(details),key=scope[4]))
    posting.db.session.commit();return 'Branding saved. Preview and select it below.'


def asset(property_id,identifier):
    try:identifier=str(uuid.UUID(str(identifier)))
    except ValueError:abort(404)
    row=next((r for r in rows(property_id) if str(r['id'])==identifier),None)
    if row is None or row['details'].get('format')!='image':abort(404)
    details=row['details'];expected=f'private/pm-soa/manager-{current_user.id}/property-{property_id}/branding-{identifier}/preview.png'
    if details['preview_key']!=expected:abort(409)
    payload=storage.r2.read_file_from_r2(expected,bucket=storage.destination())
    if len(payload)!=details['size'] or hashlib.sha256(payload).hexdigest()!=details['sha256']:raise ValueError('Branding integrity verification failed.')
    return payload
