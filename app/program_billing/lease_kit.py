"""Read-only DIY Lease Kit selections over existing particulars and approved layouts."""
from copy import deepcopy
from flask import abort,request,url_for
from . import lease_certificate as certificate,lease_certificate_form as form

COMPONENTS=(('cover','Cover'),('certificate','Certificate'),('core','Core'),
            ('special_conditions','Special Conditions'),('signature','Signature'),('lease','Lease'))
ORDER=('cover','certificate','core','special_conditions','signature')


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
    names=cover_names(items)
    def representative_lines(party,label):
        lines=[]
        for key,title in (('signatory_name',label+' Representative'),
                          ('signatory_identity_number','Representative ID Number')):
            value=party.get(key)
            if certificate.present(value):lines.append(title+': '+str(value).strip())
        return lines
    landlords=[names['landlords']]+representative_lines(items.get('_parties',{}).get('lessor',{}),'Lessor')
    tenants=[names['tenants']]
    for party in certificate.selected_tenants(items):
        tenants.extend(representative_lines(party,'Lessee'))
    return [{'kind':'paragraph','text':text} for text in ('\n'.join(landlords),'&','\n'.join(tenants))]


def component_states(items):
    from .lease_standard import special_conditions
    conditions=special_conditions(items)
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
    complete=all(states[key]['available'] for key in ORDER)
    states['lease']={'available':complete,'printable':complete,'state':'draft' if complete else 'unavailable',
                     'reason':'The complete lease is unavailable until Cover, Certificate, Core, Special Conditions and Signature are all independently available.'}
    return states


def view(property_id):
    from .lease_certificate_view import context
    from .lease_management import render_pm_template
    prop,draft,items,owners,tenants=context(property_id,allow_blank=False)
    items=developing_items(items,owners)
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
    return render_pm_template('program_billing/lease_kit_view.html',title='View Lease',property=prop,
        lease_draft=draft,viewer_only=True,kit_view=True,document_token='',selected_component=selected,
        components=[{'key':key,'label':label,'url':url_for('billing_bp.lease_live_view',component=key,**arguments)} for key,label in COMPONENTS],
        selected_label=dict(COMPONENTS)[selected],component_labels=dict(COMPONENTS),states=states,print_available=states[selected]['printable'],
        email_unavailable='Email is not yet available for this preview. Print or save it as PDF. Existing document email remains available in Lease History.',
        sections=form.render_rows(sections,values),unresolved=unresolved,preview=True,cover_blocks=cover_blocks(items),
        assembly_order=ORDER,selected_documents=[key for key in ORDER if not states[key].get('omitted')] if selected=='lease' and states['lease']['available'] else [] if states[selected].get('omitted') else [selected],
        capture_url=url_for('billing_bp.lease_certificate_edit',**arguments),
        back_url=url_for('billing_bp.pm_lease_management',**arguments),unavailable=None)
