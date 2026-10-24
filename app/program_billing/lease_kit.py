"""Read-only DIY Lease Kit selections over existing particulars and approved layouts."""
from copy import deepcopy
import re
from flask import abort,request,url_for
from . import lease_certificate as certificate,lease_certificate_form as form

COMPONENTS=(('cover','Cover'),('certificate','Certificate'),('core','Core Lease'),
            ('special_conditions','Special Conditions'),('signature','Signatures'),('lease','Full Lease'))
ORDER=('cover','certificate','core','special_conditions','signature')


def action_documents(property_id, draft_id):
    """Fixed row order; each action addresses its own protected PDF, never viewer state."""
    return [{'key': key, 'label': 'Full Lease Agreement' if key == 'lease' else label,
             'pdf_url': url_for('billing_bp.lease_kit_pdf', property_id=property_id, draft_id=draft_id, component=key),
             'email_url': url_for('billing_bp.lease_kit_email', property_id=property_id, draft_id=draft_id, component=key)}
            for key, label in COMPONENTS]


def developing_items(items,owners):
    """Live owner corrections apply to working previews; stored/signed snapshots are untouched."""
    result=deepcopy(items)
    if not owners:return result
    saved=result.get('_contracting_lessors',{}).get('identities',[])
    if not saved:
        saved=[{key:row.get(key) for key in ('owner_id','type','name','identity_number')} for row in owners]
    else:
        for party in saved:
            matches=[row for row in owners if party.get('owner_id') is not None and str(row.get('owner_id'))==str(party['owner_id'])]
            if not matches and party.get('identity_number'):
                matches=[row for row in owners if row.get('identity_number')==party['identity_number']]
            if len(matches)==1:
                for key in ('name','identity_number','type'):party[key]=matches[0].get(key) or ''
    result.setdefault('_contracting_lessors',{})['identities']=saved
    representative=result.get('_parties',{}).get('lessor',{})
    matches=[row for row in owners if representative.get('owner_id') is not None and str(row.get('owner_id'))==str(representative['owner_id'])]
    if not matches and representative.get('signatory_identity_number'):
        matches=[row for row in owners if row.get('identity_number')==representative['signatory_identity_number']]
    if len(matches)==1:
        representative['signatory_name']=matches[0].get('name') or ''
        representative['signatory_identity_number']=matches[0].get('identity_number') or ''
    return result


def cover_blocks(items):
    """Cover presentation only; saved parties and internal version records are untouched."""
    from .lease_standard import cover_names
    # Snapshots may differ in metadata/contact fields while identifying the same Lessee.
    def identity(party,key):
        value=party.get(key)
        if not certificate.present(value):return ''
        return re.sub(r'[^a-z0-9]','',str(value).casefold())
    def same_lessee(left,right):
        left_id=identity(left,'identity_number');right_id=identity(right,'identity_number')
        register_id=identity(left,'tenant_id')
        same_register=bool(register_id and register_id==identity(right,'tenant_id'))
        if left_id and right_id:
            if left_id!=right_id:return False
            if same_register:return True
            left_type=left.get('party_type');right_type=right.get('party_type')
            return not (left_type and right_type and left_type!=right_type)
        return same_register
    groups=[]
    for party in certificate.selected_tenants(items):
        group=next((entry for entry in groups if same_lessee(entry['party'],party)),None)
        if group is None:
            group={'party':deepcopy(party),'representatives':[]};groups.append(group)
        for key,value in party.items():
            if not certificate.present(group['party'].get(key)) and certificate.present(value):group['party'][key]=value
        representative={key:party.get(key) for key in ('signatory_name','signatory_identity_number')}
        if any(certificate.present(value) for value in representative.values()):
            fingerprint=tuple(' '.join(str(representative[key] or '').split()).casefold() for key in representative)
            if not any(saved[0]==fingerprint for saved in group['representatives']):
                group['representatives'].append((fingerprint,representative))
    distinct=[group['party'] for group in groups]
    cover_items=deepcopy(items)
    cover_items.setdefault('_parties',{})['lessees']=distinct
    names=cover_names(cover_items)
    def representative_lines(party,label):
        lines=[]
        for key,title in (('signatory_name',label+' Representative'),
                          ('signatory_identity_number','Representative ID Number')):
            value=party.get(key)
            if certificate.present(value):lines.append(title+': '+str(value).strip())
        return lines
    landlords=[names['landlords']]+representative_lines(items.get('_parties',{}).get('lessor',{}),'Lessor')
    tenants=['Lessee: '+names['tenants']]
    for group in groups:
        for _,representative in group['representatives']:
            tenants.extend(representative_lines(representative,'Lessee'))
    return [{'kind':'paragraph','text':text} for text in ('\n'.join(landlords),'&','\n'.join(tenants))]


def certificate_display_rows(sections, values, items, owners=(), tenants=()):
    """Share the same read-only party blocks between Certificate and assembled PDFs."""
    from .lease_certificate_parties import blocks
    rows=form.render_rows(sections,values)
    selected=certificate.selected_tenants(items)
    for party in selected:
        if not party.get('tenant_reference'):
            party['tenant_reference']=next((row.get('tenant_reference','') for row in tenants if str(row['id'])==str(party.get('tenant_id'))),'')
    rows[0]['rows']=[{'label':'Tenant Reference','parts':[{'token':'[TENANT_REFERENCE]','value':' | '.join(row.get('tenant_reference','') for row in selected if row.get('tenant_reference'))}]}]
    rows[0]['party_blocks']=blocks(items,owners,tenants)
    # Read-only Certificate rows contain values separated by pipes, never empty tokens.
    for section in rows:
        for row in section['rows']:
            populated=[part for part in row['parts'] if part.get('token') and certificate.present(part.get('value'))]
            row['parts']=[]
            for part in populated:
                if row['parts']:row['parts'].append({'text':' | '})
                row['parts'].append(part)
    return rows


def component_states(items):
    from .lease_supplementary_conditions import legacy_entries
    legacy=legacy_entries(items)
    conditions='\n'.join(entry['text'] for entry in legacy)
    states={
        'cover':{'available':True,'printable':True,'state':'draft'},
        'certificate':{'available':True,'printable':True,'state':'draft'},
        'core':{'available':False,'printable':False,'state':'unavailable',
                'reason':'An independently approved Core Terms document is not yet available. The full R01 reference has not been substituted.'},
        'special_conditions':{'available':False,'printable':bool(conditions),'state':'draft' if conditions else 'unavailable',
                'text':conditions,'reason':'An independently approved Special Conditions document is not yet available. Any saved particulars below are draft text only.'},
        'signature':{'available':False,'printable':False,'state':'unavailable',
                'reason':'An independently approved Signature page is not yet available. No signing wording or authority has been invented.'},
    }
    from .lease_supplementary_conditions import preview as supplementary_preview
    supplementary=supplementary_preview(items)
    if supplementary is not None:states['special_conditions']=supplementary
    states['core']={'available':True,'printable':True,'state':'controlled'}
    states['signature']={'available':True,'printable':True,'state':'review'}
    if supplementary is None and not conditions:
        states['special_conditions']={'available':True,'printable':True,'state':'none','omitted':True,
            'clauses':[],'reason':'Supplementary Conditions: None'}
    elif states['special_conditions'].get('omitted'):
        states['special_conditions'].update(available=True,printable=True,reason='Supplementary Conditions: None')
    if legacy:
        states['special_conditions'].update(available=False,printable=False,omitted=False,
            state='legacy_review_required',legacy_conditions=legacy,
            reason='Saved legacy Supplementary Conditions require review before PDF assembly. No conditions have been discarded or rewritten. Affected sources: '+', '.join(entry['source'] for entry in legacy))
    complete=all(states[key]['available'] for key in ORDER)
    states['lease']={'available':complete,'printable':complete,'state':'draft' if complete else 'unavailable',
                     'reason':'The complete lease is unavailable until Cover, Certificate, Core, Special Conditions and Signature are all independently available.'}
    if legacy:states['lease']['reason']=states['special_conditions']['reason']
    return states


def preview_context(property_id):
    from .lease_certificate_view import context
    prop,draft,items,owners,tenants=context(property_id,allow_blank=False)
    saved_party_items=deepcopy(items)
    selected=request.args.get('component','cover')
    if selected not in dict(COMPONENTS):abort(404)
    states=component_states(items)
    frozen=any(entry.get('kind')=='signed_original' for entry in items.get('_lease_history',{}).get('documents',[])) or any(
        generation.get('signature_or_issuance_approved') is True for generation in
        items.get('_generations',[])+items.get('_ait_standard',{}).get('generations',[]))
    values=certificate.document_values(items,None if frozen else dict(prop),owners,tenants)
    sections=form.certificate_layout(*form.counts(items))
    unresolved=[token for token in form.tokens(sections) if not certificate.present(values.get(token))]
    arguments={'property_id':property_id,'draft_id':draft['id'] if draft else None}
    from .lease_certificate_parties import blocks
    from .lease_signing import rows as signing_particulars
    party_blocks=blocks(saved_party_items,owners,tenants)
    signing_rows=signing_particulars(saved_party_items,owners,tenants)
    lessors=[party for group in party_blocks if group['label']=='Lessor' for party in group['parties']]
    lessees=[party for group in party_blocks if group['label']=='Lessee' for party in group['parties']]
    return dict(title='View Lease',property=prop,signature_lessors=lessors,signature_lessees=lessees,party_blocks=party_blocks,signing_rows=signing_rows,
        lessor_representative=items.get('_parties',{}).get('lessor',{}),
        lease_reference=values.get('[LEASE_REFERENCE]') or ('LEASE-'+str(draft['id']) if draft else ''),
        tenant_reference=' | '.join(row.get('tenant_reference') or next((t.get('tenant_reference','') for t in tenants if str(t['id'])==str(row.get('tenant_id'))),'') for row in certificate.selected_tenants(items)),
        premises_name=values.get('[PROPERTY_NAME]',''),premises_address=values.get('[PROPERTY_ADDRESS]',''),
        pdf_url=url_for('billing_bp.lease_kit_pdf',component=selected,**arguments),
        pdf_email_url=url_for('billing_bp.lease_kit_email',component=selected,**arguments),
        core_pdf_url=url_for('billing_bp.lease_kit_pdf',component='core',**arguments),
        pdf_view=selected in ('core','lease'),
        lease_draft=draft,viewer_only=True,kit_view=True,document_token='',selected_component=selected,
        components=[{'key':key,'label':label,'url':url_for('billing_bp.lease_live_view',component=key,**arguments)} for key,label in COMPONENTS],
        selected_label=dict(COMPONENTS)[selected],component_labels=dict(COMPONENTS),states=states,print_available=states[selected]['printable'],
        sections=certificate_display_rows(sections,values,saved_party_items,owners,tenants),unresolved=unresolved,preview=True,
        assembly_order=ORDER,selected_documents=list(ORDER) if selected=='lease' else [selected],
        capture_url=url_for('billing_bp.lease_certificate_edit',**arguments),
        back_url=url_for('billing_bp.pm_lease_management',**arguments),unavailable=None)


def view(property_id):
    from .lease_management import render_pm_template
    return render_pm_template('program_billing/lease_kit_view.html', **preview_context(property_id))
