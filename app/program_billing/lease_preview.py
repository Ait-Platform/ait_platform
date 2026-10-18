"""Read-only, in-memory text view of the actual merged Stage 5 Word lease."""
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
                result.append({'kind':'heading' if heading else 'paragraph','text':text})
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


def merged_preview(items):
    from app.program_billing import lease_workflow as workflow
    from app.program_billing.lease_generation import generate_preview,generate_review
    values=workflow.stage5_placeholder_values(items)
    if not workflow.stage5_completion(items):
        warnings=workflow.parties_generation_warnings(items)
        return generate_review(values,incomplete_particulars=bool(warnings),particulars_warnings=warnings)['docx']
    # Missing optional execution details remain visible in incomplete previews.
    for token in workflow.SIGNATURE_TOKENS:
        if values.get(token)=='________________':values[token]=''
    return generate_preview(values)


@billing_bp.get('/billing/leases/property/<int:property_id>/view',endpoint='lease_live_view')
@snapshot.endpoint
def view(property_id):
    from app.program_billing import lease_workflow as workflow
    prop,reason,draft=workflow.selected_context(property_id)
    if reason:abort(503,description=reason)
    if not draft:abort(400,description='Save lease particulars and select the saved draft before viewing.')
    try:blocks=extract_blocks(merged_preview(draft['items']))
    except (OSError,ValueError,KeyError,TypeError,BadZipFile,ET.XMLSyntaxError):
        abort(503,description='The saved lease text could not be displayed. Check saved particulars and master availability.')
    return render_pm_template('program_billing/lease_live_view.html',title='View Lease',property=prop,
        lease_draft=draft,lease_blocks=blocks)


@billing_bp.after_request
def private_preview(response):
    if request.endpoint=='billing_bp.lease_live_view':
        response.headers['Cache-Control']='private, no-store'
        response.headers['X-Content-Type-Options']='nosniff'
        response.headers['Referrer-Policy']='no-referrer'
    return response
