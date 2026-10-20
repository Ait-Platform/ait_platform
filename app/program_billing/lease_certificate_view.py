"""Scoped approved-certificate preview integration. Imported by lease_preview routes."""
from copy import deepcopy
from io import BytesIO
from flask import abort,request,url_for,current_app,send_file
from app.extensions import db
from app.program_billing import lease_snapshot as snapshot,lease_certificate as certificate


COMPONENT_LIMITATION=('Separate approved Core Terms and Signing components are not available. '
    'The complete R01 reference has not been substituted for them. Special Conditions and Signing remain separate; '
    'complete assembly is pending those approved components.')


def context(property_id):
    prop=snapshot.authorised_property(property_id)
    draft=None
    if request.args.get('draft_id'):
        snapshot.require_particulars_schema()
        draft=snapshot.draft_row(property_id,request.args['draft_id'])
    blank=request.args.get('blank')=='1' or draft is None
    items={} if blank else deepcopy(draft['items'])
    owners=[];tenants=[]
    history=items.get('_lease_history',{}).get('documents',[])
    frozen=any(e.get('kind')=='signed_original' for e in history) or any(
        g.get('signature_or_issuance_approved') is True for g in
        items.get('_generations',[])+items.get('_ait_standard',{}).get('generations',[]))
    if items and not frozen:
        from app.program_billing import lease_workflow as workflow,tenant_register
        owners=workflow.owners_for(property_id)
        selected={p.get('tenant_id') for p in certificate.selected_tenants(items)}
        # Read selected register identities without revalidating an already-saved snapshot.
        tenants=[dict(r) for r in tenant_register.rows() if r['id'] in selected]
    return prop,draft,items,owners,tenants


def payload(property_id):
    prop,draft,items,owners,tenants=context(property_id)
    output,unresolved=certificate.document(items,dict(prop) if items else None,owners,tenants)
    return prop,draft,output,unresolved


def pdf(property_id):
    _,_,output,_=payload(property_id)
    try:rendered=certificate.render_pdf(output)
    except Exception as error:
        from app.program_billing.lease_certificate_render import RenderUnavailable
        if not isinstance(error,RenderUnavailable):raise
        current_app.logger.warning('Certificate render unavailable error=%s',type(error).__name__)
        abort(503,description='The approved Word certificate PDF renderer is unavailable. The Word preview remains downloadable.')
    return send_file(BytesIO(rendered),mimetype='application/pdf',as_attachment=False,
        download_name='AIT-Lease-Certificate-DRAFT.pdf',conditional=False,etag=False,max_age=0)


def word(property_id):
    _,_,output,_=payload(property_id)
    return send_file(BytesIO(output),mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        as_attachment=True,download_name='AIT-Lease-Certificate-DRAFT.docx',conditional=False,etag=False,max_age=0)


def view(property_id):
    from app.program_billing.lease_management import render_pm_template
    from app.program_billing import lease_standard as standard,lease_preview as preview
    prop,draft,items,owners,tenants=context(property_id)
    _,unresolved=certificate.document(items,dict(prop) if items else None,owners,tenants)
    cover=[];cover_issue=None
    if draft and items.get('_ait_standard'):
        # Use the existing builder unchanged, retaining its actual source/styles.
        # Extract only its existing cover for the existing HTML cover renderer.
        try:
            state=items['_ait_standard']
            source,original=standard.source_master(property_id,state.get('source'))
            built=standard.build_document(items,state,source,original)
            for block in preview.extract_blocks(built):
                if block['kind']=='page_break':break
                cover.append(block)
        except Exception as error:
            # Property authorization already succeeded; a missing private cover
            # must not hide the independent approved blank certificate.
            current_app.logger.warning('Existing cover unavailable error=%s',type(error).__name__)
            cover_issue='The existing cover source is unavailable; no replacement cover was created.'
    else:
        cover_issue='An existing cover is not yet available for this draft. The approved blank certificate is available independently.'
    arguments={'property_id':property_id,'draft_id':draft['id'] if draft else None}
    if request.args.get('blank')=='1':arguments['blank']='1'
    return render_pm_template('program_billing/lease_certificate_view.html',title='View Lease',
        property=prop,lease_draft=draft,viewer_only=True,document_token='',
        cover_blocks=cover,cover_issue=cover_issue,component_limitation=COMPONENT_LIMITATION,
        certificate_pdf=url_for('billing_bp.lease_certificate_pdf',**arguments),
        certificate_word=url_for('billing_bp.lease_certificate_word',**arguments),
        existing_view=url_for('billing_bp.lease_live_view',property_id=property_id,
            draft_id=draft['id'],existing='1') if draft else None,
        unresolved=unresolved,unavailable=None)
