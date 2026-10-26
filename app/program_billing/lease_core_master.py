"""One published Core PDF source, immutable R2 versions and per-draft pins.

The legacy approved object is version 1. Publication manifests are immutable,
sequential keys: conditional PUT is the cross-worker publication lock. No
current-pointer overwrite, application migration or competing wording source.
"""
from copy import deepcopy
from datetime import datetime,timezone
from hashlib import sha256
from io import BytesIO
import json,uuid,re
from flask import abort,request,current_app,send_file,redirect,url_for,flash
from flask_login import current_user
from sqlalchemy import text
from app.extensions import db
PREFIX='private/leases/core-master/'
DOCX='application/vnd.openxmlformats-officedocument.wordprocessingml.document'


def require_admin():
    # Same active-user + ApprovedAdmin grant policy as Platform administration.
    from app.models.auth import User,ApprovedAdmin
    user=db.session.query(User).filter_by(id=current_user.id).populate_existing().with_for_update().first() if current_user.is_authenticated else None
    if user is None or not user.is_active:abort(403)
    grant=ApprovedAdmin.query.filter(db.func.lower(ApprovedAdmin.email)==user.email.lower(),db.cast(ApprovedAdmin.active,db.String).in_(['true','1'])).populate_existing().first()
    if grant is None:abort(403)
    return user


def legacy():
    from .lease_kit_pdf import CORE_SHA256,CORE_KEY
    return dict(version=1,sha256=CORE_SHA256,storage_key=CORE_KEY,pages=9,size_bytes=100272,published_at=None,published_by=None,status='published',legacy=True)


def manifest_key(version):return PREFIX+'published/'+str(int(version)).zfill(10)+'.json'


def list_keys(prefix):
    from . import lease_r2 as r2
    client,bucket=r2.r2_client()
    return [entry['Key'] for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket,Prefix=prefix) for entry in page.get('Contents',[])]


def read_json(key):
    from . import lease_r2 as r2
    return json.loads(r2.read_file_from_r2(key))


def publications():
    keys=sorted(key for key in list_keys(PREFIX+'published/') if re.fullmatch(re.escape(PREFIX)+r'published/\d{10}\.json',key))
    rows=[legacy()]
    for key in keys:
        row=read_json(key)
        if row.get('status')!='published' or type(row.get('version')) is not int or row['version']<=1 or manifest_key(row['version'])!=key:raise ValueError('Core publication manifest is invalid.')
        rows.append(row)
    return rows


def current():
    keys=sorted(key for key in list_keys(PREFIX+'published/') if re.fullmatch(re.escape(PREFIX)+r'published/\d{10}\.json',key))
    if not keys:return legacy()
    row=read_json(keys[-1])
    if row.get('status')!='published' or type(row.get('version')) is not int or row['version']<=1 or manifest_key(row['version'])!=keys[-1]:raise ValueError('Invalid current Core publication.')
    return row


def pin_new(items):
    result=deepcopy(items)
    if '_core_master' not in result:result['_core_master']=current()
    return result


def pinned(draft):return (draft or {}).get('items',{}).get('_core_master') or legacy()


def read_object(record,extension):
    from . import lease_r2 as r2
    if record.get('legacy'):
        from .lease_kit_pdf import core_bytes
        return core_bytes()
    key=record['storage_key']
    if not re.fullmatch(re.escape(PREFIX)+r'objects/[0-9a-f-]{36}\.'+extension,key):raise ValueError('Invalid Core storage scope.')
    payload=r2.read_file_from_r2(key)
    if len(payload)!=record['size_bytes'] or sha256(payload).hexdigest()!=record['sha256']:raise ValueError('Core master integrity verification failed.')
    return payload


def core_for(draft):return read_object(pinned(draft),'pdf')


def upload_object(payload,mime,extension):
    from . import lease_storage as storage,lease_r2 as r2
    key=PREFIX+'objects/'+str(uuid.uuid4())+'.'+extension
    storage.upload(key,payload,mime)
    if r2.read_file_from_r2(key)!=payload:raise ValueError('Core master storage read-back failed.')
    return dict(storage_key=key,sha256=sha256(payload).hexdigest(),size_bytes=len(payload))


def working_copies():
    return [read_json(key) for key in sorted(list_keys(PREFIX+'working/')) if re.fullmatch(re.escape(PREFIX)+r'working/[0-9a-f-]{36}\.json',key)]


def prepare(upload):
    from . import lease_storage as storage
    actor=require_admin();base=current()
    if upload is None:raise ValueError('Select a working DOCX document.')
    name,mime,payload,digest=storage.validate_master(upload)
    record=dict(upload_object(payload,mime,'docx'),id=str(uuid.uuid4()),filename=name,base_version=base['version'],prepared_at=datetime.now(timezone.utc).isoformat(),prepared_by=int(actor.id))
    storage.upload(PREFIX+'working/'+record['id']+'.json',json.dumps(record,sort_keys=True).encode(),'application/json')
    return record


def publish(identifier,upload):
    from . import lease_storage as storage
    actor=require_admin()
    try:identifier=str(uuid.UUID(identifier))
    except (ValueError,TypeError):raise ValueError('Select a saved working DOCX.')
    work=read_json(PREFIX+'working/'+identifier+'.json');base=current()
    if work['base_version']!=base['version']:raise ValueError('The published Core changed. Prepare the correction against the current version before publishing.')
    read_object(work,'docx')
    if upload is None:raise ValueError('Select the corrected PDF rendered from the working DOCX.')
    name,extension,mime,payload,digest=storage.validate_upload(upload)
    if extension!='.pdf':raise ValueError('Publish a PDF rendered from the saved working DOCX.')
    from pypdf import PdfReader
    pages=len(PdfReader(BytesIO(payload)).pages)
    record=dict(upload_object(payload,mime,'pdf'),version=base['version']+1,pages=pages,published_at=datetime.now(timezone.utc).isoformat(),published_by=int(actor.id),status='published',legacy=False,source_docx=work,previous_version=base['version'])
    # Unique immutable key makes concurrent publications conflict rather than overwrite.
    storage.upload(manifest_key(record['version']),json.dumps(record,sort_keys=True).encode(),'application/json')
    return record


def update_draft(property_id,identifier,expected_version):
    actor=require_admin();master=current()
    try:identifier=uuid.UUID(str(identifier));expected_version=int(expected_version)
    except (ValueError,TypeError):abort(400)
    draft=db.session.execute(text('SELECT * FROM bil_lease_draft WHERE id=:d AND property_id=:p FOR UPDATE'),{'d':identifier,'p':property_id}).mappings().first()
    if not draft:abort(404)
    items=deepcopy(draft['items'])
    signed=any(entry.get('kind','').startswith('signed_') for entry in items.get('_lease_history',{}).get('documents',[]))
    approved=any(g.get('signature_or_issuance_approved') for g in items.get('_generations',[])+items.get('_ait_standard',{}).get('generations',[]))
    if draft['status']!='draft' or signed or approved:abort(409,description='Signed or issued lease particulars cannot change Core version.')
    if draft['version']!=expected_version:abort(409,description='The draft changed. Reopen it before updating its Core version.')
    previous=pinned(draft);items['_core_master']=master
    items.setdefault('_lease_history',{'documents':[],'emails':[]}).setdefault('core_master_updates',[]).append(dict(previous=previous,new_version=master['version'],updated_by=int(actor.id),updated_at=datetime.now(timezone.utc).isoformat()))
    db.session.execute(text('UPDATE bil_lease_draft SET items=CAST(:items AS jsonb),version=version+1,updated_at=now() WHERE id=:d AND version=:v'),{'items':json.dumps(items),'d':identifier,'v':expected_version});db.session.commit()


