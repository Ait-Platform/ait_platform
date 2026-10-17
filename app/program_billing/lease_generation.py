"""Private pinned Stage 5 master and pure draft generation. No app startup or writes."""
import hashlib
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile
from lxml import etree as ET
from .lease_merge import merge_draft_docx, TOKEN, W

MASTER_SHA256 = '98b391c497cb2c70f931a6117a3b0074776256faaa8be91f3b6b9f4ad5d12937'
MASTER_VERSION = 'Stage5-' + MASTER_SHA256[:12]
MASTER_PATH = Path(__file__).with_name('private_templates') / 'Lease_Master_Stage5_Review.docx'
STATUS = 'DRAFT \u2014 FOR REVIEW\nNOT FOR SIGNATURE OR ISSUANCE'


def load_master():
    data = MASTER_PATH.read_bytes()
    if hashlib.sha256(data).hexdigest() != MASTER_SHA256:
        raise ValueError('Packaged Stage 5 master integrity verification failed.')
    return data


def notice(size, status=STATUS):
    p = ET.Element('{'+W+'}p')
    props = ET.SubElement(p,'{'+W+'}pPr')
    ET.SubElement(props,'{'+W+'}keepNext')
    ET.SubElement(props,'{'+W+'}spacing',{'{'+W+'}after':'160'})
    run = ET.SubElement(p,'{'+W+'}r')
    rp = ET.SubElement(run,'{'+W+'}rPr')
    ET.SubElement(rp,'{'+W+'}b')
    ET.SubElement(rp,'{'+W+'}color',{'{'+W+'}val':'9C0006'})
    ET.SubElement(rp,'{'+W+'}sz',{'{'+W+'}val':str(size)})
    for i,line in enumerate(status.splitlines()):
        if i:ET.SubElement(run,'{'+W+'}br')
        ET.SubElement(run,'{'+W+'}t').text=line
    return p


def mark_draft(payload, status=STATUS):
    output = BytesIO()
    with ZipFile(BytesIO(payload)) as source, ZipFile(output,'w') as target:
        for entry in source.infolist():
            content = source.read(entry)
            if entry.filename=='word/document.xml' or (entry.filename.startswith('word/header') and entry.filename.endswith('.xml')):
                root = ET.fromstring(content,parser=ET.XMLParser(resolve_entities=False,no_network=True))
                parent = root.find('{'+W+'}body') if entry.filename=='word/document.xml' else root
                parent.insert(0,notice(24 if entry.filename=='word/document.xml' else 16, status))
                content=ET.tostring(root,encoding='utf-8',xml_declaration=True)
            target.writestr(entry,content)
    return output.getvalue()


def generate_review(values, *, incomplete_particulars=False, particulars_warnings=None):
    if len(values)!=37:raise ValueError('All 37 Stage 5 particulars must be supplied.')
    merged=merge_draft_docx(load_master(),values,approved_sha256=MASTER_SHA256)
    warning = 'Lessor service address not supplied \u2014 complete before signing.'
    warnings = list(particulars_warnings) if particulars_warnings is not None else [warning] if incomplete_particulars else []
    status = 'DRAFT \u2014 INCOMPLETE PARTICULARS\nNOT FOR SIGNATURE OR ISSUANCE\n' + '\n'.join(warnings) if incomplete_particulars else STATUS
    payload=mark_draft(merged['docx'], status)
    with ZipFile(BytesIO(payload)) as archive:
        for name in archive.namelist():
            if name.startswith('word/') and name.endswith('.xml'):
                root=ET.fromstring(archive.read(name),parser=ET.XMLParser(resolve_entities=False,no_network=True))
                for p in root.iter('{'+W+'}p'):
                    if TOKEN.search(''.join(n.text or '' for n in p.iter('{'+W+'}t'))):
                        raise ValueError('Generated document contains unresolved merge placeholders.')
    return dict(docx=payload,master_sha256=MASTER_SHA256,master_version=MASTER_VERSION,
                output_sha256=hashlib.sha256(payload).hexdigest(),merged_values=dict(values),incomplete_particulars=bool(incomplete_particulars),
                warnings=warnings,
                status='draft-incomplete-particulars-not-for-issuance' if incomplete_particulars else 'draft-for-review-not-for-issuance')
