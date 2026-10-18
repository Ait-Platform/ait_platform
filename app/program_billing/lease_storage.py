"""Private lease storage adapter: authenticated R2 APIs only; no public URLs."""
import hashlib,os
from io import BytesIO
from pathlib import PurePosixPath
from uuid import UUID
from zipfile import ZipFile
from xml.etree import ElementTree as ET
from pypdf import PdfReader
from flask import current_app
from werkzeug.utils import secure_filename
from app.program_billing import lease_r2 as r2

MAX_BYTES=10*1024*1024
MIMES={'.pdf':'application/pdf','.docx':'application/vnd.openxmlformats-officedocument.wordprocessingml.document'}

def destination():
    return r2.settings()['bucket']


def key_for(property_id,draft_id,document_id,extension):
    if type(property_id) is not int or property_id<=0 or extension not in MIMES:raise ValueError("Invalid lease storage scope")
    return f"private/leases/property-{property_id}/draft-{UUID(str(draft_id))}/document-{UUID(str(document_id))}{extension}"

def validate_upload(file):
    filename=secure_filename(file.filename or '')
    extension=PurePosixPath(filename).suffix.lower()
    if not filename or len(filename)>200 or extension not in MIMES:raise ValueError("Select a PDF or DOCX file with a valid filename.")
    data=file.stream.read(MAX_BYTES+1)
    if not data or len(data)>MAX_BYTES:raise ValueError("Lease documents must be nonempty and no larger than 10 MB.")
    try:
        if extension=='.pdf':
            if not data.startswith(b'%PDF-'):raise ValueError()
            reader=PdfReader(BytesIO(data))
            if reader.is_encrypted or not len(reader.pages):raise ValueError()
        else:
            with ZipFile(BytesIO(data)) as archive:
                entries=archive.infolist()
                if len(entries)>2000 or sum(e.file_size for e in entries)>40*1024*1024:raise ValueError()
                names=set(archive.namelist())
                if not {'[Content_Types].xml','word/document.xml'}<=names:raise ValueError()
                if any(n.startswith(('/','\\')) or '..' in PurePosixPath(n).parts or '\\' in n or 'vbaproject' in n.lower() for n in names):raise ValueError()
                if archive.getinfo('[Content_Types].xml').file_size>1024*1024 or archive.getinfo('word/document.xml').file_size>8*1024*1024:raise ValueError()
                types=ET.fromstring(archive.read('[Content_Types].xml'))
                document=ET.fromstring(archive.read('word/document.xml'))
                if document.tag!='{http://schemas.openxmlformats.org/wordprocessingml/2006/main}document':raise ValueError()
                if not any(n.attrib.get('ContentType')=='application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml' for n in types):raise ValueError()
                # CRC verification, without extracting or executing document content.
                if archive.testzip() is not None:raise ValueError()
    except Exception as error:
        raise ValueError("The file content is not a supported, unencrypted PDF or DOCX.") from error
    return filename,extension,MIMES[extension],data,hashlib.sha256(data).hexdigest()

def upload(key,data,mime):
    bucket=destination()
    return r2.upload_bytes_to_r2(key,data,bucket=bucket,content_type=mime)

def retrieve(record):
    bucket=destination()
    extension=PurePosixPath(record['filename']).suffix.lower()
    expected=key_for(record['property_id'],record['draft_id'],record['id'],extension)
    if record['storage_key']!=expected:raise ValueError("Lease document storage scope is invalid.")
    data=r2.read_file_from_r2(expected,bucket=bucket)
    if len(data)!=record['size_bytes'] or hashlib.sha256(data).hexdigest()!=record['sha256']:
        raise ValueError("Lease document integrity verification failed.")
    return data


# Property masters are separate from draft attachments and never select a
# generation template. Reuse the same approved private R2 destination/API.
def master_key(property_id,document_id):
    if type(property_id) is not int or property_id<=0:raise ValueError('Invalid property master scope')
    return f"private/leases/property-{property_id}/masters/master-{UUID(str(document_id))}.docx"


def validate_master(file):
    from werkzeug.datastructures import FileStorage
    from lxml import etree as safe_xml
    original=(file.filename or '').replace('\\','/').rsplit('/',1)[-1]
    if not original or len(original)>200 or PurePosixPath(original).suffix.lower()!='.docx' or any(ord(c)<32 for c in original):
        raise ValueError('Select a DOCX file with a filename of at most 200 characters.')
    data=file.stream.read(MAX_BYTES+1)
    if not data or len(data)>MAX_BYTES:raise ValueError('Master documents must be nonempty and no larger than 10 MB.')
    try:
        with ZipFile(BytesIO(data)) as archive:
            entries=archive.infolist();names=[e.filename for e in entries]
            if len(entries)>2000 or len(set(names))!=len(names) or sum(e.file_size for e in entries)>40*1024*1024:
                raise ValueError()
            for entry in entries:
                if entry.flag_bits & 1:raise ValueError()
                if entry.filename.lower().endswith(('.xml','.rels')):
                    content=archive.read(entry)
                    if b'<!DOCTYPE' in content or b'<!ENTITY' in content:raise ValueError()
                    xml=safe_xml.fromstring(content,parser=safe_xml.XMLParser(resolve_entities=False,load_dtd=False,no_network=True))
                    if xml.getroottree().docinfo.doctype:raise ValueError()
    except Exception as error:
        raise ValueError('Upload a valid, unencrypted DOCX package without duplicate parts or XML entity declarations.') from error
    # Reuse the existing path/macro/content-type/XML/CRC validation without using
    # the original filename as a storage path (including non-ASCII names).
    _,_,mime,validated,digest=validate_upload(FileStorage(stream=BytesIO(data),filename='master.docx'))
    return original,mime,validated,digest


def retrieve_master(record):
    expected=master_key(record['property_id'],record['id'])
    if record['storage_key']!=expected:raise ValueError('Property master storage scope is invalid.')
    data=r2.read_file_from_r2(expected,bucket=destination())
    if len(data)!=record['size_bytes'] or hashlib.sha256(data).hexdigest()!=record['sha256']:
        raise ValueError('Property master integrity verification failed.')
    return data
