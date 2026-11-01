"""Validated evidence using the existing private PM R2 transport; no public URLs."""
import hashlib,uuid,warnings
from io import BytesIO
from pathlib import PurePosixPath
from werkzeug.utils import secure_filename
from app.program_billing import lease_r2 as transport
MAX_BYTES=10*1024*1024
MIMES={'.pdf':'application/pdf','.png':'image/png','.jpg':'image/jpeg','.jpeg':'image/jpeg'}

def validate(file):
    name=secure_filename((file.filename or '').replace('\\','/').rsplit('/',1)[-1])
    extension=PurePosixPath(name).suffix.lower()
    if not name or len(name)>200 or extension not in MIMES:raise ValueError('Evidence must be a PDF, PNG or JPEG file with a filename of at most 200 characters.')
    data=file.stream.read(MAX_BYTES+1)
    if not data or len(data)>MAX_BYTES:raise ValueError('Evidence must be nonempty and no larger than 10 MB.')
    try:
        if extension=='.pdf':
            from pypdf import PdfReader
            if not data.startswith(b'%PDF-'):raise ValueError()
            document=PdfReader(BytesIO(data))
            if document.is_encrypted or not document.pages:raise ValueError()
        else:
            from PIL import Image
            with warnings.catch_warnings():
                warnings.simplefilter('error',Image.DecompressionBombWarning)
                with Image.open(BytesIO(data)) as image:
                    if image.format!=('PNG' if extension=='.png' else 'JPEG') or image.width*image.height>30000000:raise ValueError()
                    image.verify()
    except Exception as exc:raise ValueError('Evidence content is not a valid supported PDF or image.') from exc
    return dict(filename=name,mime_type=MIMES[extension],size_bytes=len(data),sha256=hashlib.sha256(data).hexdigest(),data=data,extension=extension)

def key(p,a,e,identifier,extension):
    return f'private/pm-utilities/property-{int(p)}/account-{int(a)}/exception-{int(e)}/evidence-{uuid.UUID(str(identifier))}{extension}'

def upload(record,data):return transport.upload_bytes_to_r2(record['storage_key'],data,content_type=record['mime_type'])

def retrieve(record):
    expected=key(record['property_id'],record['account_id'],record['exception_id'],record['id'],PurePosixPath(record['filename']).suffix.lower())
    if record['storage_key']!=expected:raise ValueError('Evidence storage scope is invalid.')
    data=transport.read_file_from_r2(expected)
    if len(data)!=record['size_bytes'] or hashlib.sha256(data).hexdigest()!=record['sha256']:raise ValueError('Evidence integrity verification failed.')
    return data
