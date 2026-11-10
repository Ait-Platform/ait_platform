"""Immutable official-publication evidence, separate from selectable tariffs."""
from hashlib import sha256
from importlib import import_module
import json
import os
from pathlib import Path
import re
from uuid import uuid4

from flask import current_app
from .municipal_tariff_sources import official_url, year_start


class StorageError(ValueError):
    pass


def prefix(year):
    year_start(year)
    return 'municipal-tariffs/ethekwini/'+year.replace('/','-')+'/'


class Disk:
    def __init__(self, root):
        self.root=Path(root).resolve()
        public=[Path(current_app.root_path)/'static',Path(current_app.root_path).parent/'static']
        public.extend(Path(current_app.config[key]) for key in ('UPLOAD_FOLDER','UPLOAD_ROOT') if current_app.config.get(key))
        if current_app.static_folder:public.append(Path(current_app.static_folder))
        if any(self.root.is_relative_to(path.resolve()) for path in public):
            raise StorageError('Municipal document storage must be outside public static/upload directories.')

    def path(self, key):
        path=(self.root/key).resolve()
        if not path.is_relative_to(self.root) or path==self.root:
            raise StorageError('Municipal document storage scope is invalid.')
        return path

    def put(self, key, content, mime):
        target=self.path(key);target.parent.mkdir(parents=True,exist_ok=True)
        try:
            with target.open('xb') as stream:
                stream.write(content);stream.flush();os.fsync(stream.fileno())
        except FileExistsError:
            if target.read_bytes()!=content:raise StorageError('Stored document collision; no file was overwritten.')
        if target.read_bytes()!=content:raise StorageError('Stored document read-back verification failed.')

    def get(self, key):return self.path(key).read_bytes()

    def keys(self, scope):
        folder=self.path(scope)
        if not folder.exists():return []
        return sorted(str(path.relative_to(self.root)).replace('\\','/') for path in folder.rglob('*.json') if path.is_file())


    def receipts(self, year):
        folder=self.path(prefix(year)+'receipts')
        return sorted(folder.glob('*.json'),key=lambda path:path.stat().st_mtime,reverse=True)[:20] if folder.exists() else []

    def listing(self, year):return [self.get(str(path.relative_to(self.root))) for path in self.receipts(year)]


class R2:
    """Reuse authenticated immutable platform transport for PUBLIC municipal data.

    Only official publications and public source metadata are stored here. User
    identity is logged server-side, never written to this possibly public bucket.
    """
    def __init__(self):self.api=import_module('app.utils.cloudflare_r2')
    def put(self,key,content,mime):self.api.upload_bytes_to_r2(key,content,content_type=mime)
    def get(self,key):return self.api.read_file_from_r2(key)
    def keys(self,scope):
        client,bucket=self.api.r2_client()
        keys=[];continuation=None
        while True:
            values=dict(Bucket=bucket,Prefix=scope,MaxKeys=1000)
            if continuation:values['ContinuationToken']=continuation
            response=client.list_objects_v2(**values)
            keys.extend(row['Key'] for row in response.get('Contents',[]) if row['Key'].endswith('.json'))
            if len(keys)>10000:raise StorageError('Monitoring history exceeds supported scope; no history was discarded.')
            if not response.get('IsTruncated'):return sorted(keys)
            following=response.get('NextContinuationToken')
            if not following or following==continuation:raise StorageError('Monitoring history pagination failed.')
            continuation=following

    def listing(self,year):
        client,bucket=self.api.r2_client()
        response=client.list_objects_v2(Bucket=bucket,Prefix=prefix(year)+'receipts/',MaxKeys=1000)
        if response.get('IsTruncated'):
            raise StorageError('Municipal evidence listing exceeds its supported page; stored documents remain intact.')
        items=sorted(response.get('Contents',[]),key=lambda row:row['LastModified'],reverse=True)[:20]
        return [self.get(row['Key']) for row in items]


def backend():
    configured=current_app.config.get('PM_TARIFF_DOCUMENT_ROOT') or os.getenv('PM_TARIFF_DOCUMENT_ROOT')
    if configured:return Disk(configured)
    if current_app.testing:raise StorageError('Tests must configure isolated municipal document storage.')
    if all(os.getenv(key) for key in ('R2_ENDPOINT_URL','R2_ACCESS_KEY','R2_SECRET_KEY','R2_BUCKET_NAME')):
        return R2()
    if os.getenv('RENDER'):
        raise StorageError('Durable municipal document storage is unavailable. Use the existing configured R2 transport or set PM_TARIFF_DOCUMENT_ROOT on the existing persistent disk.')
    return Disk(Path(current_app.instance_path)/'pm_tariff_publications')


def metadata(raw,year,receipt=None):
    if len(raw)>65536:raise StorageError('Municipal source metadata exceeds its supported size.')
    try:
        value=json.loads(raw)
        if (value['financial_year']!=year or not re.fullmatch(r'[0-9a-f]{32}',value['receipt_id'])
                or receipt is not None and receipt!=value['receipt_id']
                or not re.fullmatch(r'[0-9a-f]{64}',value['sha256'])
                or not isinstance(value['size_bytes'],int) or value['size_bytes']<=0):raise ValueError()
        official_url(value['source_url'],pdf=True)
        return value
    except (ValueError,KeyError,TypeError):raise StorageError('Stored municipal source metadata is invalid.') from None


def save(store,content,details):
    year=details['financial_year'];digest=sha256(content).hexdigest()
    if details['sha256']!=digest or details['size_bytes']!=len(content):
        raise StorageError('Downloaded document does not match its source metadata.')
    record=dict(details,receipt_id=uuid4().hex)
    payload=json.dumps(record,sort_keys=True).encode('utf8')
    metadata(payload,year)
    store.put(prefix(year)+'documents/'+digest+'.pdf',content,'application/pdf')
    # Publish the receipt only AFTER the exact PDF has passed immutable read-back.
    store.put(prefix(year)+'receipts/'+record['receipt_id']+'.json',payload,'application/json')
    return record


def listing(store,year):
    return sorted([metadata(raw,year) for raw in store.listing(year)],key=lambda value:value['retrieved_at'],reverse=True)


def read(store,year,receipt):
    year_start(year)
    if not re.fullmatch(r'[0-9a-f]{32}',receipt):raise StorageError('Invalid municipal document identifier.')
    record=metadata(store.get(prefix(year)+'receipts/'+receipt+'.json'),year,receipt)
    content=store.get(prefix(year)+'documents/'+record['sha256']+'.pdf')
    if (len(content)!=record['size_bytes'] or sha256(content).hexdigest()!=record['sha256']
            or not content.startswith(b'%PDF-')):raise StorageError('Stored municipal PDF integrity verification failed.')
    return content
