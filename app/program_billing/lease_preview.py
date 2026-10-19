"""Read-only, in-memory text view of the selected property's verified uploaded Word master."""
from io import BytesIO
from zipfile import ZipFile,BadZipFile
import re
from lxml import etree as ET
from flask import abort,request
from app.program_billing.routes import billing_bp
from app.program_billing import lease_snapshot as snapshot
from app.program_billing.lease_management import render_pm_template
from app.program_billing.lease_merge import W


def paragraph_text(paragraph):
    parts=[]
    for node in paragraph.iter():
        if node.tag=='{'+W+'}t':parts.append(node.text or '')
        elif node.tag=='{'+W+'}tab':parts.append('\t')
        elif node.tag in ('{'+W+'}br','{'+W+'}cr'):parts.append('\n')
    return ''.join(parts)


def extract_blocks(payload):
    """Extract body paragraphs and table rows in XML order, including nested tables."""
    with ZipFile(BytesIO(payload)) as archive:
        document=ET.fromstring(archive.read('word/document.xml'),
            parser=ET.XMLParser(resolve_entities=False,no_network=True))
    body=document.find('{'+W+'}body')
    if body is None:raise ValueError('Missing Word document body.')
    def blocks(parent):
        result=[]
        for child in parent:
            if child.tag=='{'+W+'}p':
                text=paragraph_text(child)
                style=child.find('./{'+W+'}pPr/{'+W+'}pStyle')
                name=style.get('{'+W+'}val','') if style is not None else ''
                heading=bool(re.match(r'(?i)heading[1-6]$',name) or name.lower()=='title' or re.match(r'^PART [ABC]\b',text))
                before=child.find('./{'+W+'}pPr/{'+W+'}pageBreakBefore')
                if before is not None and before.get('{'+W+'}val','1') not in ('0','false','off'):
                    result.append({'kind':'page_break'})
                # Only explicit page breaks, not cached lastRenderedPageBreak markers.
                segments=['']
                for node in child.iter():
                    if node.tag=='{'+W+'}t':segments[-1]+=node.text or ''
                    elif node.tag=='{'+W+'}tab':segments[-1]+='\t'
                    elif node.tag=='{'+W+'}br' and node.get('{'+W+'}type')=='page':segments.append('')
                    elif node.tag in ('{'+W+'}br','{'+W+'}cr'):segments[-1]+='\n'
                for index,segment in enumerate(segments):
                    if index:result.append({'kind':'page_break'})
                    result.append({'kind':'heading' if heading else 'paragraph','text':segment})
                section=child.find('./{'+W+'}pPr/{'+W+'}sectPr')
                if section is not None:
                    kind=section.find('{'+W+'}type')
                    if kind is None or kind.get('{'+W+'}val') in ('nextPage','oddPage','evenPage'):
                        result.append({'kind':'page_break'})
            elif child.tag=='{'+W+'}tbl':
                rows=[]
                for row in child.findall('{'+W+'}tr'):
                    cells=[]
                    for cell in row.findall('{'+W+'}tc'):
                        cells.append(blocks(cell))
                    rows.append(cells)
                result.append({'kind':'table','rows':rows})
            elif child.tag in ('{'+W+'}sdt','{'+W+'}sdtContent','{'+W+'}customXml'):
                result.extend(blocks(child))
        return result
    return blocks(body)


def merged_preview(items,master,sha256):
    """Merge exact recognised tokens only; never insert a Stage 5 schedule/cover."""
    from app.program_billing import lease_workflow as workflow
    from app.program_billing.lease_merge import TOKEN,merge_draft_docx
    tokens=set()
    with ZipFile(BytesIO(master)) as archive:
        for name in archive.namelist():
            if name.startswith('word/') and name.endswith('.xml'):
                root=ET.fromstring(archive.read(name),parser=ET.XMLParser(resolve_entities=False,no_network=True))
                if any('MERGEFIELD' in (node.text or '').upper() for node in root.iter('{'+W+'}instrText')) or any(
                    'MERGEFIELD' in node.get('{'+W+'}instr','').upper() for node in root.iter('{'+W+'}fldSimple')):
                    raise ValueError('Native Word merge fields require a reviewed mapping.')
                for paragraph in root.iter('{'+W+'}p'):
                    tokens.update(TOKEN.findall(''.join(node.text or '' for node in paragraph.iter('{'+W+'}t'))))
    values=workflow.stage5_placeholder_values(items)
    supplied={token:value for token,value in values.items() if token in tokens and isinstance(value,str)
        and value.strip().strip('_') and not TOKEN.search(value)}
    if not tokens:return master
    return merge_draft_docx(master,supplied,approved_sha256=sha256,preview_only=True)['docx']


def block_text(block):
    if block['kind']=='table':return '\n'.join(block_text(b) for row in block['rows'] for cell in row for b in cell)
    return block.get('text','')


def placeholder_sources():
    from app.program_billing import lease_workflow as workflow
    sources={
        '[S1 LEGAL LANDLORD(S)]':('parties','_contracting_lessors.identities'),
        '[LANDLORD SIGNATORY NAME AND CAPACITY]':('parties','_parties.lessor.signatory_name / signatory_identity_number / signatory_capacity'),
        '[S1 LEGAL TENANT]':('parties','_parties.lessees[].legal_name'),
        '[TENANT REGISTRATION OR ID]':('parties','_parties.lessees[].identity_number'),
        '[TENANT SIGNATORY NAME AND CAPACITY]':('parties','_parties.lessees[].party_type / signatory_name / signatory_identity_number / signatory_capacity'),
        '[S1 LANDLORD SERVICE ADDRESS AND CONTACT PARTICULARS]':('parties','_parties.lessor.service_address / email / phone'),
        '[S1 TENANT SERVICE ADDRESS AND CONTACT PARTICULARS]':('parties','_parties.lessees[].address / email / phone'),
    }
    sources.update({token:('parties','_parties.signatures.'+key) for token,key in workflow.SIGNATURE_TOKENS.items()})
    sources.update({token:('premises','_premises.'+key) for token,key in workflow.PREMISES_TOKENS.items()})
    sources.update({token:('period',key+'.value') for token,key in workflow.PERIOD_TOKENS.items()})
    sources.update({token:('amounts','_amounts.'+key) for token,key in workflow.FINANCIAL_TOKENS.items()})
    sources.update({token:('amounts','_banking_utilities.'+key) for token,key in zip(workflow.ANCILLARY_TOKENS,
        ('utility_allocation_method','utility_references / utility_no_references_reason','bank_name / branch_name / branch_code / account_holder / account_type / account_number / swift'))})
    return sources


def logical_pages(blocks,source_blocks):
    """Section-based logical pages, never asserted to be Microsoft Word pages."""
    from app.program_billing.lease_merge import TOKEN
    from collections import Counter
    # Uploaded previews add no cover, schedule or notice to the source body.
    if len(blocks)!=len(source_blocks):raise ValueError('Master/merged document structure does not align.')
    annotated=[]
    for block,source in zip(blocks,source_blocks):
        annotated.append((block,TOKEN.findall(block_text(source))))
    pages=[];current=[];tokens=[]
    def finish():
        if current and any(block_text(b).strip() for b in current):
            title=next((b['text'] for b in current if b['kind']=='heading'),'Lease particulars')
            pages.append({'blocks':list(current),'title':title,'tokens':dict(Counter(tokens))})
        current.clear();tokens.clear()
    for block,found in annotated:
        if block['kind']=='page_break':finish();continue
        if block['kind']=='heading' and any(b['kind']!='heading' and not b.get('preview_notice') and block_text(b).strip() for b in current):finish()
        current.append(block);tokens.extend(found)
    finish()
    return pages


def prepared_document(property_id,draft,master_id=None):
    from app.program_billing import lease_property_master as masters,lease_storage as storage
    from app.extensions import db
    from sqlalchemy import text
    from flask import current_app
    reason=masters.readiness()
    if reason:abort(503,description=reason)
    query="SELECT * FROM bil_property_lease_master WHERE property_id=:p AND state='available' AND verified_at IS NOT NULL"
    params={'p':property_id}
    if master_id:
        from uuid import UUID
        try:params['id']=UUID(str(master_id))
        except (ValueError,TypeError):abort(404)
        query+=' AND id=:id'
    record=db.session.execute(text(query+' ORDER BY version DESC LIMIT 1'),params).mappings().first()
    if record is None:abort(503,description='No verified uploaded master is available for this property. Upload and verify a master before viewing.')
    try:
        master=storage.retrieve_master(record)
        return record,merged_preview(draft['items'],master,record['sha256'])
    except Exception as error:
        current_app.logger.warning('Property master preview failed error=%s',type(error).__name__)
        abort(503,description='The verified property master could not be retrieved, integrity-checked or displayed. No Stage 5 fallback was used.')


@billing_bp.get('/billing/leases/property/<int:property_id>/view',endpoint='lease_live_view')
@snapshot.endpoint
def view(property_id):
    from app.program_billing import lease_workflow as workflow
    prop,reason,draft=workflow.selected_context(property_id)
    if reason:abort(503,description=reason)
    if not draft:abort(400,description='Save lease particulars and select the saved draft before viewing.')
    if draft['items'].get('_ait_standard'):
        from app.program_billing import lease_standard as standard
        return standard.view(property_id)
    record,output=prepared_document(property_id,draft)
    blocks=extract_blocks(output)
    pages=logical_pages(blocks,blocks)
    from app.program_billing.lease_history import view_token
    token=view_token(draft,'working',output,master_id=str(record['id']))
    return render_pm_template('program_billing/lease_live_view.html',title='View Lease',property=prop,
        lease_draft=draft,lease_blocks=blocks,logical_pages=pages,viewer_only=True,document_token=token)


@billing_bp.after_request
def private_preview(response):
    if request.endpoint in ('billing_bp.lease_live_view','billing_bp.lease_pdf_status'):
        response.headers['Cache-Control']='private, no-store'
        response.headers['X-Content-Type-Options']='nosniff'
        response.headers['Referrer-Policy']='no-referrer'
    return response


@billing_bp.get('/billing/leases/property/<int:property_id>/generate-pdf',endpoint='lease_pdf_status')
@snapshot.endpoint
def pdf_status(property_id):
    from app.program_billing import lease_workflow as workflow
    prop,reason,draft=workflow.selected_context(property_id)
    if reason:abort(503,description=reason)
    return render_pm_template('program_billing/lease_pdf_status.html',title='Generate PDF',property=prop,lease_draft=draft)
