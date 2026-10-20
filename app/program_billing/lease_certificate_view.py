"""Scoped approved-certificate preview integration. Imported by lease_preview routes."""
from copy import deepcopy
from io import BytesIO
from flask import abort,request,url_for,current_app,send_file,redirect,flash
from datetime import datetime,timezone
import uuid
from flask_login import current_user
from . import lease_certificate_form as form
from app.extensions import db
from app.program_billing import lease_snapshot as snapshot,lease_certificate as certificate


COMPONENT_LIMITATION=('Separate approved Core Terms and Signing components are not available. '
    'The complete R01 reference has not been substituted for them. Special Conditions and Signing remain separate; '
    'complete assembly is pending those approved components.')


def context(property_id):
    prop=snapshot.authorised_property(property_id,request.method=='POST')
    draft=None
    if request.values.get('draft_id'):
        snapshot.require_particulars_schema()
        draft=snapshot.draft_row(property_id,request.values['draft_id'],request.method=='POST')
    blank=(request.method=='GET' and request.args.get('blank')=='1') or draft is None
    items={} if blank else deepcopy(draft['items'])
    owners=[];tenants=[]
    history=items.get('_lease_history',{}).get('documents',[])
    frozen=any(e.get('kind')=='signed_original' for e in history) or any(
        g.get('signature_or_issuance_approved') is True for g in
        items.get('_generations',[])+items.get('_ait_standard',{}).get('generations',[]))
    if not frozen:
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
    from app.program_billing import lease_workflow as workflow,lease_property_payments as payments,tenant_register
    frozen = payments.signed(draft) if draft else False
    saved_items = draft['items'] if draft else {}
    frozen = frozen or any(g.get('signature_or_issuance_approved') is True for g in
        saved_items.get('_generations',[])+saved_items.get('_ait_standard',{}).get('generations',[]))
    bank_reason = payments.readiness()
    linked = [] if bank_reason else payments.accounts(property_id)
    errors = []
    values = certificate.values(items,None if frozen else dict(prop),owners,tenants)
    lessor_count,lessee_count = form.counts(items)
    if request.method == 'POST':
        snapshot.require_particulars_schema()
        if frozen:abort(409,description='Issued or signed lease particulars are preserved. Use Addendum for changes.')
        try:
            if draft and str(draft['version']) != request.form.get('version'):
                abort(409,description='The draft changed. Reload before saving.')
            lessor_count,lessee_count = form.form_counts(request.form)
            sections = form.layout(lessor_count,lessee_count)
            edits = form.submitted(request.form,sections)
            pending = form.merge(items,edits,lessor_count,lessee_count)
            action = request.form.get('action','save')
            bank_id = request.form.get('bank_detail_id','')
            saved_id = str(certificate.bank_snapshot(items).get('bank_detail_id') or '')
            if bank_id != saved_id:
                if bank_reason:raise ValueError('Bank selection is unavailable. Saved particulars have not changed.')
                account = next((row for row in linked if str(row['bank_detail_id']) == bank_id),None)
                if bank_id and account is None:raise ValueError('Choose an account already linked to this property.')
                if account:
                    pending['_certificate_banking'] = {key:account[key] for key in certificate.BANK_FIELDS}
                    pending['_certificate_banking'].update(schema_version=1,bank_detail_id=account['bank_detail_id'],
                        property_id=property_id,selected_by=int(current_user.id),
                        selected_at=datetime.now(timezone.utc).isoformat())
                else:pending['_certificate_banking']={'schema_version':1,'property_id':property_id,'bank_detail_id':None}
            if action == 'save':
                if draft:workflow.save_items(draft,pending)
                else:draft=workflow.create_parties_draft(property_id,pending)
                flash('Lease certificate saved. Missing particulars remain incomplete; signing authority has not been automatically confirmed.','success')
                return redirect(url_for('billing_bp.lease_live_view',property_id=property_id,draft_id=draft['id']))
            if action not in ('add_lessor','remove_lessor','add_lessee','remove_lessee'):abort(400)
            values.update(edits)
            if action == 'add_lessor':lessor_count+=1
            elif action == 'remove_lessor':lessor_count=max(1,lessor_count-1)
            elif action == 'add_lessee':lessee_count+=1
            elif action == 'remove_lessee':lessee_count=max(1,lessee_count-1)
            if max(lessor_count,lessee_count)>form.MAX_PARTIES:raise ValueError('A maximum of 50 parties per type is supported.')
        except ValueError as error:
            errors.append(str(error))
            lessor_count=max(1,min(form.MAX_PARTIES,lessor_count))
            lessee_count=max(1,min(form.MAX_PARTIES,lessee_count))
            # Keep entered values visible after validation errors; nothing was saved.
            for token in form.tokens(form.layout(lessor_count,lessee_count)):
                if not form.readonly(token) and token[1:-1] in request.form:values[token]=request.form[token[1:-1]]
    sections = form.layout(lessor_count,lessee_count)
    unresolved = [token for token in form.tokens(sections) if not certificate.present(values.get(token))]
    sections = form.render_rows(sections,values)
    # Register choices are read only and scoped by the existing portfolio checks.
    tenant_choices = [] if frozen else [{key:row.get(key) for key in
        ('legal_name','identity_number','signatory_name','signatory_identity_number','signatory_capacity')}
        for row in tenant_register.rows()]

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
    return render_pm_template('program_billing/lease_certificate_view.html',title='Lease Certificate',
        property=prop,lease_draft=draft,viewer_only=True,document_token='',
        cover_blocks=cover,cover_issue=cover_issue,component_limitation=COMPONENT_LIMITATION,
        certificate_pdf=url_for('billing_bp.lease_certificate_pdf',**arguments),
        certificate_word=url_for('billing_bp.lease_certificate_word',**arguments),
        existing_view=url_for('billing_bp.lease_live_view',property_id=property_id,
            draft_id=draft['id'],existing='1') if draft else None,
        unresolved=unresolved,unavailable=None,sections=sections,errors=errors,frozen=frozen,
        preview=request.args.get('mode')=='preview' or request.args.get('blank')=='1',lessor_count=lessor_count,lessee_count=lessee_count,
        linked_accounts=linked,bank_reason=bank_reason,
        selected_bank=str(request.form.get('bank_detail_id',certificate.bank_snapshot(items).get('bank_detail_id') or '')),
        registration_token=request.form.get('registration_token') or str(uuid.uuid4()),
        owner_choices=[{key:row.get(key) for key in ('name','identity_number')} for row in owners],tenant_choices=tenant_choices,
        certificate_edit=url_for('billing_bp.lease_live_view',property_id=property_id,draft_id=draft['id'] if draft else None),
        certificate_preview=url_for('billing_bp.lease_live_view',mode='preview',**arguments)),400 if errors else 200
