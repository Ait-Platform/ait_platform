"""Opt-in AIT Standard Lease development workspace; no legal release is approved.

Existing drafts and property masters remain authoritative. No startup writes,
schema changes, operational leases, public storage or automatic fallback.
"""
from copy import deepcopy
from datetime import datetime,timezone
from hashlib import sha256
from io import BytesIO
import json,re,uuid
from pathlib import Path
from zipfile import ZipFile
from lxml import etree as ET
from flask import abort,request,redirect,url_for,flash,send_file,current_app
from flask_login import current_user
from sqlalchemy import text
from app.extensions import db
from app.program_billing.routes import billing_bp
from app.program_billing import lease_snapshot as snapshot,lease_workflow as workflow
from app.program_billing.lease_management import render_pm_template
from app.program_billing.lease_merge import W,TOKEN,merge_draft_docx
from app.program_billing import lease_preview as preview

ROOT=Path(__file__).parent
DICTIONARY=json.loads((ROOT/'lease_standard_fields.json').read_text(encoding='utf-8'))
RELEASE=json.loads((ROOT/'lease_standard_release.json').read_text(encoding='utf-8'))
DISCLAIMER_VERSION='ait-standard-notice-development-001'
DISCLAIMER_TITLE='AIT STANDARD LEASE \u2014 IMPORTANT NOTICE'
DISCLAIMER=('The AIT Standard Lease is provided as a general contractual template. AIT does not guarantee its suitability for every property, tenant or transaction.\n'
    'The user is responsible for verifying the accuracy of all particulars, confirming the identity and authority of contracting parties, reviewing the agreement and ensuring compliance with applicable law.\n'
    'Independent legal advice should be obtained where appropriate.\n'
    'Generating a lease through AIT does not, by itself, establish that the agreement is legally valid or enforceable.')
DISCLAIMER_HASH=sha256((DISCLAIMER_TITLE+'\n'+DISCLAIMER).encode()).hexdigest()


def digest(value):
    return sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,default=str).encode()).hexdigest()


def particulars_digest(items):
    return digest({k:v for k,v in items.items() if k not in ('_ait_standard','_generations','_lease_history')})


def authority_parties(items):
    parties=items.get('_parties',{})
    lessor=parties.get('lessor',{})
    result=[{'key':'landlords','name':'All contracting landlords','representative':lessor.get('signatory_name',''),
        'identity_number':lessor.get('signatory_identity_number',''),'capacity':lessor.get('signatory_capacity','')}]
    for index,tenant in enumerate(parties.get('lessees',[]) or [parties.get('lessee',{})]):
        if tenant.get('party_type')=='juristic_person' or tenant.get('signatory_name'):
            result.append({'key':'tenant-'+str(index),'name':tenant.get('legal_name',''),
                'representative':tenant.get('signatory_name',''),'identity_number':tenant.get('signatory_identity_number',''),
                'capacity':tenant.get('signatory_capacity','')})
    for party in result:
        represented=items.get('_contracting_lessors',{}).get('identities',[]) if party['key']=='landlords' else parties.get('lessees',[]) or [parties.get('lessee',{})]
        party['fingerprint']=digest({'representative':party,'represented':represented})
    return result


def source_master(property_id,pin=None):
    from app.program_billing import lease_property_master as masters,lease_storage as storage
    problem=masters.readiness()
    if problem:raise ValueError(problem)
    query="SELECT * FROM bil_property_lease_master WHERE property_id=:p AND state='available' AND verified_at IS NOT NULL"
    params={'p':property_id}
    if pin:
        query+=' AND id=:id';params['id']=pin['id']
    row=db.session.execute(text(query+' ORDER BY version DESC LIMIT 1'),params).mappings().first()
    if row is None:raise ValueError('No verified R01 source master is available for this property.')
    if pin and (str(row['id'])!=pin['id'] or row['sha256']!=pin['sha256'] or row['version']!=pin['version']):
        raise ValueError('The pinned source version has changed; the standard working copy was not replaced.')
    if row['original_filename']!='Lease_Review_Draft_R01.docx':
        raise ValueError('Select a verified Lease_Review_Draft_R01.docx development reference. No other master was substituted.')
    payload=storage.retrieve_master(row)
    return dict(row),payload


def inventory(payload):
    """Inventory is descriptive only; it never authorises exclusions."""
    clauses={};references=set();tokens=set();duplicates=[]
    with ZipFile(BytesIO(payload)) as archive:
        for name in archive.namelist():
            if not name.startswith('word/') or not name.endswith('.xml'):continue
            raw=archive.read(name)
            if b'<!DOCTYPE' in raw or b'<!ENTITY' in raw:raise ValueError('Unsupported document XML.')
            root=ET.fromstring(raw,parser=ET.XMLParser(resolve_entities=False,no_network=True))
            if any('MERGEFIELD' in (node.text or '').upper() for node in root.iter('{'+W+'}instrText')) or any(
                'MERGEFIELD' in node.get('{'+W+'}instr','').upper() for node in root.iter('{'+W+'}fldSimple')):
                raise ValueError('Native Word MERGEFIELD instructions require a reviewed dictionary.')
            for paragraph in root.iter('{'+W+'}p'):
                value=preview.paragraph_text(paragraph)
                tokens.update(TOKEN.findall(value))
                if name=='word/document.xml':
                    match=re.match(r'^\s*(\d+(?:\.\d+)*)(?:\.?\s+)(\S.*)',value)
                    if match:
                        key=match[1]
                        if key in clauses:duplicates.append(key)
                        else:clauses[key]={'heading':value[:200],'sha256':sha256(value.encode()).hexdigest()}
                    references.update(re.findall(r'(?i)\bclauses?\s+(\d+(?:\.\d+)*)',value))
    return {'clauses':clauses,'tokens':sorted(tokens),'unresolved_references':sorted(references-clauses.keys()),'ambiguous_numbers':sorted(set(duplicates))}


def exclusions_check(state,source,info):
    schedule=state.get('applicability',{})
    if schedule.get('terms_sha256')!=source['sha256']:raise ValueError('Applicability schedule does not match the exact terms version.')
    if schedule.get('excluded'):
        # No legally reviewed version-specific register has been supplied.
        # This release deliberately cannot exclude any clause, even via a forged request.
        raise ValueError('All clauses are mandatory. Exclusions require an approved, exact-version clause register and cross-reference review.')
    if schedule.get('clause_inventory_sha256')!=digest(info):raise ValueError('Clause inventory changed; exclusions and readiness remain blocked.')


def acknowledged(state,items):
    ack=state.get('acknowledgement',{})
    return (ack.get('version')==DISCLAIMER_VERSION and ack.get('sha256')==DISCLAIMER_HASH
        and ack.get('particulars_sha256')==particulars_digest(items)
        and ack.get('terms_sha256')==state.get('source',{}).get('sha256') and bool(ack.get('by')) and bool(ack.get('at')))


def saved_particulars(items):
    """Read compatibility only: selected tenant records win over a legacy mirror."""
    result=deepcopy(items)
    parties=result.get('_parties',{})
    for tenant in parties.get('lessees',[]):
        for key in workflow.PARTY_FIELDS['lessee']:tenant.setdefault(key,'')
    if parties.get('lessees'):parties['lessee']=deepcopy(parties['lessees'][0])
    return result


def certificate_values(items):
    values=workflow.stage5_placeholder_values(saved_particulars(items))
    return {field['token']:values.get(field['token'],'') for field in DICTIONARY['fields']}


def cover_names(items):
    """Display abbreviations only; never mutate or replace legal party identities."""
    def short(party,key):
        if party.get('short_name'):return party['short_name']
        words=party.get(key,'').split()
        if not words:return '[PARTY NAME]'
        natural=(party.get('type') or party.get('party_type')) in ('individual','natural_person')
        if natural and len(words)>1:return ' '.join(word[0].upper()+'.' for word in words[:-1])+' '+words[-1]
        return ''.join(word[0].upper()+'.' for word in words) if len(words)>1 else words[0]
    parties=items.get('_parties',{})
    return {'landlords':'; '.join(short(p,'name') for p in items.get('_contracting_lessors',{}).get('identities',[])) or '[LANDLORDS]',
        'tenants':'; '.join(short(p,'legal_name') for p in parties.get('lessees',[]) or [parties.get('lessee',{})])}


def certificate_rows(items):
    values=certificate_values(items)
    rows=[dict(field,value=values.get(field['token']) if values.get(field['token'],'').strip().strip('_') else field['token']) for field in DICTIONARY['fields']]
    for field in DICTIONARY.get('certificate_additional_fields',[]):
        if field['key']=='duration_months':value=workflow.period_duration_months(items)
        elif field['key']=='renewal_notice_date':value=items.get('_period',{}).get('renewal_notice_date','')
        else:value=special_conditions(items) if field['key']=='special_conditions' else items.get('_premises',{}).get(field['key'],'')
        rows.append(dict(field,value=value or '[Not supplied: '+field['label']+']'))
    return rows


def special_conditions(items):
    values=[items.get('sc-'+str(i).zfill(2),{}).get('value','') for i in range(1,11)]
    values += [str(c.get('number',''))+' | '+c.get('text','') for c in items.get('_additional_clauses',[]) if isinstance(c,dict)]
    return '\n'.join(value for value in values if value.strip())


def readiness(items,state,source,info,documents=None):
    issues=[]
    try:exclusions_check(state,source,info)
    except ValueError as error:issues.append(str(error))
    if not acknowledged(state,items):issues.append('Acknowledge the current notice for these particulars and terms.')
    if not (RELEASE['terms_approved'] and RELEASE['disclaimer_approved'] and RELEASE['clause_policy_approved']
        and RELEASE['approved_terms_sha256']==source['sha256'] and RELEASE['approved_disclaimer_sha256']==DISCLAIMER_HASH
        and RELEASE.get('review_reference') and RELEASE.get('clause_policy_terms_sha256')==source['sha256']
        and RELEASE.get('clause_inventory_sha256')==digest(info) and not RELEASE.get('excludable_clauses')
        and RELEASE.get('clause_register')==info['clauses']):
        issues.append('Terms, disclaimer and exact-version clause policy await legal release approval. Ready for Signature is blocked.')
    if not info['clauses']:issues.append('No reliable source clause inventory is available.')
    values=certificate_values(items)
    for field in DICTIONARY['fields']:
        value=values[field['token']]
        if not field['execution_only'] and (not value or not value.strip().strip('_') or TOKEN.search(value)):
            issues.append('Complete '+field['key'].replace('_',' ')+'.')
    for party in authority_parties(items):
        authority=state.get('authorities',{}).get(party['key'],{})
        if not (party['representative'] and party['identity_number'] and party['capacity'] and authority.get('confirmed')
            and authority.get('evidence') and authority.get('party_fingerprint')==party['fingerprint']):
            issues.append('Confirm identity, capacity and authority evidence for '+party['name']+'.')
    issues.extend(message for _,_,message in workflow.stage5_completion(items)
        if 'no merge slot' not in message and 'different master' not in message)
    issues.extend(message for _,message in workflow.reference_alerts(items))
    if documents is None:issues.append('Supporting document presence has not been checked.')
    else:issues.extend(message for group,key,message in workflow.completion_alerts(items,documents) if group=='supporting')
    if info['unresolved_references'] or info['ambiguous_numbers']:issues.append('Source clause numbering and cross-references require review.')
    if set(info['tokens'])-values.keys():issues.append('Source contains unmapped fields; complete the reviewed dictionary before signature readiness.')
    return list(dict.fromkeys(issues))


def element(tag):return ET.Element('{'+W+'}'+tag)


def paragraph(value,style=None):
    p=element('p')
    if style:
        props=ET.SubElement(p,'{'+W+'}pPr');ET.SubElement(props,'{'+W+'}pStyle').set('{'+W+'}val',style)
    for index,line in enumerate(value.split('\n')):
        run=ET.SubElement(p,'{'+W+'}r')
        if index:ET.SubElement(run,'{'+W+'}br')
        node=ET.SubElement(run,'{'+W+'}t');node.set('{http://www.w3.org/XML/1998/namespace}space','preserve');node.text=line
    return p


def build_document(items,state,source,payload):
    """The same deterministic DOCX is used by both View and Word download.

    Preserve the full reference body and package, including schedules/signatures.
    Section-boundary extraction needs actual R01 review; no clause is guessed away.
    """
    info=inventory(payload);exclusions_check(state,source,info)
    with ZipFile(BytesIO(payload)) as archive:
        root=ET.fromstring(archive.read('word/document.xml'),parser=ET.XMLParser(resolve_entities=False,no_network=True))
        body=root.find('{'+W+'}body')
        if body is None:raise ValueError('Source has no document body.')
        names=cover_names(items)
        front=[paragraph('AIT Standard Lease','Title'),paragraph('Between '+names['landlords']+' and '+names['tenants']),
            paragraph('DRAFT \u2014 DEVELOPMENT WORKING COPY\nNOT FOR SIGNATURE OR ISSUANCE'),
            paragraph('Terms version '+state['terms_version']),paragraph('Lease Certificate','Heading1')]
        table=element('tbl')
        for field in DICTIONARY['fields']:
            row=ET.SubElement(table,'{'+W+'}tr')
            for value in (field['label'],field['token']):
                cell=ET.SubElement(row,'{'+W+'}tc');cell.append(paragraph(value))
        for field in certificate_rows(items)[len(DICTIONARY['fields']):]:
            if field['key']=='special_conditions':continue  # its own saved section below
            row=ET.SubElement(table,'{'+W+'}tr')
            for value in (field['label'],field['value']):
                cell=ET.SubElement(row,'{'+W+'}tc');cell.append(paragraph(value))
        front.append(table)
        front.extend([paragraph('Contracting Parties and Representatives','Heading1')])
        for owner in items.get('_contracting_lessors',{}).get('identities',[]):
            front.append(paragraph('Landlord: '+owner.get('name','[LANDLORD NAME]')+' \u2014 ID / registration: '+owner.get('identity_number','[LANDLORD ID]')))
        parties=items.get('_parties',{})
        for tenant in parties.get('lessees',[]) or [parties.get('lessee',{})]:
            front.append(paragraph('Tenant: '+tenant.get('legal_name','[TENANT NAME]')+' \u2014 ID / registration: '+tenant.get('identity_number','[TENANT ID]')))
        for party in authority_parties(items):
            authority=state.get('authorities',{}).get(party['key'],{})
            confirmed=authority.get('confirmed') and authority.get('party_fingerprint')==party['fingerprint']
            front.append(paragraph(party['name']+' representative: '+(party['representative'] or '[REPRESENTATIVE NAME]')+
                '\nID: '+(party['identity_number'] or '[REPRESENTATIVE ID]')+'\nCapacity: '+(party['capacity'] or '[CAPACITY]')+
                '\nAuthority: '+(authority.get('evidence','') if confirmed else '[AUTHORITY NOT CONFIRMED]')))
        front.extend([paragraph('Special Conditions','Heading1'),paragraph(special_conditions(items) or 'No additional conditions recorded.'),
            paragraph('Clause Applicability Schedule','Heading1'),paragraph('All source clauses apply. No exclusions are approved.'),
            paragraph('AIT Core Terms and Conditions','Heading1'),paragraph('Development reference only: operative core terms have not been separately approved. Original reference wording is retained unchanged.')])
        for index,node in enumerate(front):body.insert(index,node)
        stream=BytesIO()
        with ZipFile(stream,'w') as target:
            for entry in archive.infolist():
                target.writestr(entry,ET.tostring(root,encoding='utf-8',xml_declaration=True) if entry.filename=='word/document.xml' else archive.read(entry.filename))
    values=certificate_values(items)
    supplied={token:value for token,value in values.items() if isinstance(value,str) and value.strip().strip('_') and not TOKEN.search(value)}
    template=stream.getvalue()
    return merge_draft_docx(template,supplied,approved_sha256=sha256(template).hexdigest(),preview_only=True)['docx']


def context(property_id,lock=False):
    prop,reason,draft=workflow.selected_context(property_id,lock)
    if reason:abort(503,description=reason)
    if not draft:abort(400,description='Save lease particulars and select a draft first.')
    return prop,draft


def get_source(property_id,state):
    try:
        source,payload=source_master(property_id,state.get('source'))
        return source,payload,inventory(payload)
    except Exception as error:
        current_app.logger.warning('AIT standard source unavailable error=%s',type(error).__name__)
        abort(503,description='The pinned verified R01 reference could not be retrieved or checked. No other master was substituted.')


@billing_bp.route('/billing/leases/property/<int:property_id>/standard',methods=['GET','POST'],endpoint='lease_standard')
@snapshot.endpoint
def workspace(property_id):
    prop,draft=context(property_id,request.method=='POST')
    items=deepcopy(draft['items']);state=items.get('_ait_standard',{})
    source,payload,info=get_source(property_id,state)
    errors=[]
    if request.method=='POST':
        if request.form.getlist('exclude_clauses'):abort(400,description='All clauses are mandatory; no exclusions are approved.')
        if request.form.get('acknowledge')!='on':errors.append('Acknowledge the important notice before proceeding.')
        if not errors:
            authorities={}
            for party in authority_parties(items):
                evidence=request.form.get('authority_'+party['key'],'').strip()
                if len(evidence)>2000:abort(400,description='Authority evidence must not exceed 2000 characters.')
                authorities[party['key']]={'confirmed':request.form.get('confirm_'+party['key'])=='on' and bool(evidence) and all(party[k] for k in ('representative','identity_number','capacity')),
                    'evidence':evidence,'party_fingerprint':party['fingerprint']}
            state=deepcopy(state)
            pin={'id':str(source['id']),'version':source['version'],'sha256':source['sha256'],'property_id':property_id}
            state.update(schema_version=1,source=pin,terms_version='ait-working-'+source['sha256'][:16],
                dictionary_version=DICTIONARY['version'],status='draft',authorities=authorities,
                applicability={'terms_sha256':source['sha256'],'clause_inventory_sha256':digest(info),'excluded':[]})
            history=list(state.get('acknowledgements',[]))
            if len(history)>=100:abort(409,description='Notice acknowledgement history limit reached; preserve the history before further review.')
            ack={'version':DISCLAIMER_VERSION,'sha256':DISCLAIMER_HASH,'terms_sha256':source['sha256'],
                'particulars_sha256':particulars_digest(items),'by':int(current_user.id),'at':datetime.now(timezone.utc).isoformat()}
            history.append(ack);state.update(acknowledgement=ack,acknowledgements=history)
            if request.form.get('request_ready')=='on':
                if readiness(items,state,source,info,snapshot.documents(draft)):abort(400,description='Ready for Signature remains blocked by the listed pre-signature checks.')
                state['status']='ready-for-signature'
                state['ready_particulars_sha256']=particulars_digest(items)
            items['_ait_standard']=state;workflow.save_items(draft,items)
            flash('AIT Standard Lease working copy saved. Lease remains a draft.','success')
            return redirect(url_for('billing_bp.lease_standard',property_id=property_id,draft_id=draft['id']))
    issues=readiness(items,state,source,info,snapshot.documents(draft))
    return render_pm_template('program_billing/lease_standard.html',title='AIT Standard Lease',property=prop,lease_draft=draft,
        standard=state,source=source,inventory=info,certificate_fields=certificate_rows(items),notice_title=DISCLAIMER_TITLE,notice=DISCLAIMER,notice_version=DISCLAIMER_VERSION,
        representatives=authority_parties(items),issues=issues,errors=errors,can_review=acknowledged(state,items),
        standard_status='ready-for-signature' if state.get('status')=='ready-for-signature' and not issues else 'draft',unavailable=None),400 if errors else 200


def prepared(property_id,lock=False):
    prop,draft=context(property_id,lock);state=draft['items'].get('_ait_standard',{})
    if not acknowledged(state,draft['items']):abort(400,description='Acknowledge the current notice in the AIT Standard Lease workspace first.')
    source,payload,info=get_source(property_id,state)
    try:output=build_document(draft['items'],state,source,payload)
    except Exception as error:
        current_app.logger.warning('AIT standard merge failed error=%s',type(error).__name__)
        abort(503,description='The pinned standard working copy could not be merged. No other master was substituted.')
    return prop,draft,state,source,output


@billing_bp.get('/billing/leases/property/<int:property_id>/standard/view',endpoint='lease_standard_view')
@snapshot.endpoint
def view(property_id):
    prop,draft,state,source,output=prepared(property_id)
    blocks=preview.extract_blocks(output)
    pages=preview.logical_pages(blocks,blocks)
    from app.program_billing.lease_history import view_token
    token=view_token(draft,'standard',output)
    return render_pm_template('program_billing/lease_live_view.html',title='View AIT Standard Lease',property=prop,lease_draft=draft,
        logical_pages=pages,lease_blocks=blocks,viewer_only=True,document_token=token,
        back_url=url_for('billing_bp.lease_standard',property_id=property_id,draft_id=draft['id']))


@billing_bp.post('/billing/leases/property/<int:property_id>/standard/word',endpoint='lease_standard_word')
@snapshot.endpoint
def word(property_id):
    prop,draft,state,source,output=prepared(property_id,True)
    history=list(state.get('generations',[]))
    if len(history)>=100:abort(409,description='Generation history limit reached; existing history must be preserved.')
    history.append({'id':str(uuid.uuid4()),'by':int(current_user.id),'at':datetime.now(timezone.utc).isoformat(),
        'terms_version':state['terms_version'],'source_sha256':source['sha256'],'source_version':source['version'],
        'dictionary_version':DICTIONARY['version'],'source_draft_version':draft['version'],
        'particulars_sha256':particulars_digest(draft['items']),'values':certificate_values(draft['items']),
        'particulars_snapshot':deepcopy({k:v for k,v in draft['items'].items() if k not in ('_ait_standard','_generations','_lease_history')}),
        'authority_snapshot':deepcopy(state.get('authorities',{})),'applicability_snapshot':deepcopy(state.get('applicability',{})),
        'acknowledgement_snapshot':deepcopy(state.get('acknowledgement',{})),
        'output_sha256':sha256(output).hexdigest(),'status':'draft','signature_or_issuance_approved':False})
    items=deepcopy(draft['items']);items['_ait_standard']['generations']=history
    workflow.save_items(draft,items)
    return send_file(BytesIO(output),mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        as_attachment=True,download_name='AIT-Standard-Lease-DRAFT.docx',conditional=False,etag=False,max_age=0)


@billing_bp.get('/billing/leases/property/<int:property_id>/standard/terms.pdf',endpoint='lease_core_terms_pdf')
@snapshot.endpoint
def core_terms_pdf(property_id):
    prop,draft=context(property_id)
    state=draft['items'].get('_ait_standard',{})
    if not acknowledged(state,draft['items']):abort(400,description='Acknowledge the current development notice in working-copy settings first.')
    if request.args.get('revision') and request.args['revision']!=str(draft['version']):
        abort(409,description='Saved particulars changed. Reopen the Lease workspace for the current PDF.')
    source,payload,info=get_source(property_id,state)
    try:
        exclusions_check(state,source,info)
        from app.program_billing.lease_terms_pdf import build_pdf
        output=build_pdf(payload,draft['items'],source)
    except Exception as error:
        current_app.logger.warning('Lease terms PDF unavailable error=%s',type(error).__name__)
        abort(503,description='The pinned development reference could not be rendered safely. No other document was substituted.')
    return send_file(BytesIO(output),mimetype='application/pdf',as_attachment=True,
        download_name='AIT-Core-Terms-DEVELOPMENT-DRAFT.pdf',conditional=False,etag=False,max_age=0)


@billing_bp.after_request
def private_response(response):
    if request.endpoint in ('billing_bp.lease_standard','billing_bp.lease_standard_view','billing_bp.lease_standard_word','billing_bp.lease_core_terms_pdf'):
        response.headers['Cache-Control']='private, no-store'
        response.headers['X-Content-Type-Options']='nosniff'
        response.headers['Referrer-Policy']='same-origin'
    return response
