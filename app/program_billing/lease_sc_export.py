"""Read-only, in-memory Word export of one explicitly selected saved lease draft."""
from datetime import datetime
from io import BytesIO
import re
from xml.etree import ElementTree as ET
from zipfile import ZipFile, ZIP_DEFLATED
from zoneinfo import ZoneInfo
from flask import abort, request, send_file
from . import lease_snapshot as snapshot

W = 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'
ET.register_namespace('w', W)


def text(value):
    if value is None:return ''
    if not isinstance(value, str):raise ValueError('Saved condition text is not a string.')
    if any(not (c in '\t\r\n' or 0x20 <= ord(c) <= 0xD7FF or 0xE000 <= ord(c) <= 0xFFFD or 0x10000 <= ord(c) <= 0x10FFFF) for c in value):
        raise ValueError('Saved condition text contains characters Word cannot represent.')
    return value


def saved_clauses(items):
    """No templates, normalization, deduplication or inferred contract wording."""
    saved=items.get('_supplementary_conditions', {})
    if not isinstance(saved,dict):raise ValueError('Saved Special Conditions require review.')
    clauses=saved.get('clauses',[])
    if not isinstance(clauses,list):raise ValueError('Saved Special Conditions require review.')
    rows=[]
    for index,clause in enumerate(clauses,1):
        if not isinstance(clause,dict):raise ValueError('Saved clause requires review.')
        number=clause.get('number')
        number='SC-%02d' % index if number is None else str(number)
        rows.append((number,text(clause.get('heading')),text(clause.get('text')),False))
    for key in sorted(key for key in items if re.fullmatch(r'sc-\d+',key)):
        value=items[key]
        if isinstance(value,dict):
            if 'value' not in value:raise ValueError('Saved legacy condition requires review.')
            value=value['value']
        if value is not None and value!='':rows.append((key,'',text(value),True))
    additional=items.get('_additional_clauses',[])
    if not isinstance(additional,list):raise ValueError('Saved additional clauses require review.')
    for clause in additional:
        if not isinstance(clause,dict):raise ValueError('Saved additional clause requires review.')
        rows.append((str(clause.get('number','')),text(clause.get('heading')),text(clause.get('text')),True))
    amounts=items.get('_amounts',{})
    if not isinstance(amounts,dict):raise ValueError('Saved condition particulars require review.')
    for key in ('rental_adjustments_text','parking_other_charges_text','utilities_allocation_text'):
        value=amounts.get(key)
        if value is not None and value!='':rows.append(('',key,text(value),True))
    return rows


def build_word(prop,draft,exported_at):
    items=draft['items'];rows=saved_clauses(items)
    document=ET.Element('{'+W+'}document');body=ET.SubElement(document,'{'+W+'}body')
    def paragraph(value,bold=False,size='22'):
        p=ET.SubElement(body,'{'+W+'}p');pr=ET.SubElement(p,'{'+W+'}pPr')
        ET.SubElement(pr,'{'+W+'}spacing',{'{'+W+'}after':'120'})
        r=ET.SubElement(p,'{'+W+'}r');rp=ET.SubElement(r,'{'+W+'}rPr')
        ET.SubElement(rp,'{'+W+'}rFonts',{'{'+W+'}ascii':'Arial','{'+W+'}hAnsi':'Arial'})
        ET.SubElement(rp,'{'+W+'}sz',{'{'+W+'}val':size})
        if bold:ET.SubElement(rp,'{'+W+'}b')
        for part in re.split(r'(\r\n|\n|\r|\t)',text(value)):
            if part=='\t':ET.SubElement(r,'{'+W+'}tab')
            elif part in ('\n','\r','\r\n'):ET.SubElement(r,'{'+W+'}br')
            elif part:ET.SubElement(r,'{'+W+'}t',{'{http://www.w3.org/XML/1998/namespace}space':'preserve'}).text=part
    paragraph('Special Conditions',True,'32')
    paragraph('Property: '+text(prop.get('name'))+' (ID '+str(prop['id'])+')')
    parties=items.get('_parties',{});tenants=parties.get('lessees') if 'lessees' in parties else [parties.get('lessee',{})]
    if isinstance(tenants,list):
        names=[text(party.get('legal_name')) for party in tenants if isinstance(party,dict) and party.get('legal_name')]
        if names:paragraph('Tenant: '+'; '.join(names))
    paragraph('Lease draft ID: '+str(draft['id']))
    paragraph('Draft version: '+str(draft['version']))
    paragraph('Export date: '+exported_at.isoformat(timespec='seconds')+' (Africa/Johannesburg)')
    if items.get('_supplementary_conditions',{}).get('mode')=='none':paragraph('Saved Special Conditions selection: None. Retained saved clauses follow, if present.')
    legacy=False
    for number,heading,content,older in rows:
        if older and not legacy:paragraph('Other saved condition particulars',True,'26');legacy=True
        paragraph(number+(' — ' if number and heading else '')+heading,True)
        paragraph(content)
    if not rows:paragraph('No saved Special Conditions.')
    sect=ET.SubElement(body,'{'+W+'}sectPr')
    ET.SubElement(sect,'{'+W+'}pgSz',{'{'+W+'}w':'11906','{'+W+'}h':'16838'})
    ET.SubElement(sect,'{'+W+'}pgMar',{ '{'+W+'}'+key:'1134' for key in ('top','right','bottom','left')})
    output=BytesIO()
    with ZipFile(output,'w',ZIP_DEFLATED) as archive:
        archive.writestr('[Content_Types].xml','<?xml version="1.0" encoding="UTF-8"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/></Types>')
        archive.writestr('_rels/.rels','<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/></Relationships>')
        archive.writestr('word/document.xml',ET.tostring(document,encoding='utf-8',xml_declaration=True))
    return output.getvalue()


def word(property_id):
    prop=snapshot.authorised_property(property_id)
    identifier=request.args.get('draft_id')
    if not identifier:abort(400,description='Select a saved lease draft before exporting.')
    snapshot.require_particulars_schema()
    draft=snapshot.draft_row(property_id,identifier)
    try:payload=build_word(prop,draft,datetime.now(ZoneInfo('Africa/Johannesburg')))
    except ValueError:abort(409,description='The saved Special Conditions could not be exported without altering their contents. Review the saved data.')
    response=send_file(BytesIO(payload),mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        as_attachment=True,download_name='AIT-Special-Conditions-Property-'+str(property_id)+'-'+str(draft['id'])+'.docx',conditional=False,etag=False,max_age=0)
    response.headers['Cache-Control']='private, no-store'
    response.headers['Pragma']='no-cache'
    response.headers['X-Content-Type-Options']='nosniff'
    return response
