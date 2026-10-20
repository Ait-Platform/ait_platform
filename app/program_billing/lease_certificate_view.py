"""Scoped approved-certificate preview integration. Imported by lease_preview routes."""
from copy import deepcopy
from io import BytesIO
from flask import abort,request,url_for,current_app,send_file,redirect,flash
from datetime import datetime,timezone
import uuid,re
from flask_login import current_user
from . import lease_certificate_form as form
from app.extensions import db
from app.program_billing import lease_snapshot as snapshot,lease_certificate as certificate


COMPONENT_LIMITATION=('Separate approved Core Terms and Signing components are not available. '
    'The complete R01 reference has not been substituted for them. Special Conditions and Signing remain separate; '
    'complete assembly is pending those approved components.')


def context(property_id,allow_blank=True):
    prop=snapshot.authorised_property(property_id,request.method=='POST')
    draft=None
    if request.values.get('draft_id'):
        snapshot.require_particulars_schema()
        draft=snapshot.draft_row(property_id,request.values['draft_id'],request.method=='POST')
    blank=(allow_blank and request.method=='GET' and request.args.get('blank')=='1') or draft is None
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


def edit(property_id):
    from app.program_billing.lease_management import render_pm_template
    prop,draft,items,owners,tenants=context(property_id)
    from .lease_kit import developing_items
    items=developing_items(items,owners)
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
            _,lessee_count = form.form_counts(request.form)
            lessor_count=form.counts(items)[0]
            sections = form.layout(lessor_count,lessee_count)
            edits = form.submitted(request.form,sections)
            edits = {token:value for token,value in edits.items() if not re.fullmatch(r'\[LESSOR_\d+_(?:NAME|IDENTITY_NUMBER)\]',token)}
            pending = form.merge(items,edits,lessor_count,lessee_count)
            original_rep=items.get('_parties',{}).get('lessor',{})
            proposed_rep=pending.get('_parties',{}).get('lessor',{})
            if any(original_rep.get(key,'')!=proposed_rep.get(key,'') for key in
                   ('signatory_name','signatory_identity_number','signatory_capacity')):
                proposed_rep['capacity_confirmed']=False
            if original_rep.get('signatory_identity_number','')!=proposed_rep.get('signatory_identity_number',''):
                proposed_rep.pop('owner_id',None)
                if 'identity_source' in proposed_rep:proposed_rep['identity_source']='lease_particulars'

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
                return redirect(url_for('billing_bp.lease_certificate_edit',property_id=property_id,draft_id=draft['id']))
            if action not in ('add_lessee','remove_lessee'):abort(400)
            values.update(edits)
            if action == 'add_lessee':lessee_count+=1
            elif action == 'remove_lessee':lessee_count=max(1,lessee_count-1)
            if max(lessor_count,lessee_count)>form.MAX_PARTIES:raise ValueError('A maximum of 50 parties per type is supported.')
        except ValueError as error:
            errors.append(str(error))
            lessor_count=max(1,min(form.MAX_PARTIES,lessor_count))
            lessee_count=max(1,min(form.MAX_PARTIES,lessee_count))
            # Keep entered values visible after validation errors; nothing was saved.
            for token in form.tokens(form.layout(lessor_count,lessee_count)):
                if not form.readonly(token) and not re.fullmatch(r'\[LESSOR_\d+_(?:NAME|IDENTITY_NUMBER)\]',token) and token[1:-1] in request.form:values[token]=request.form[token[1:-1]]
    sections = form.layout(lessor_count,lessee_count)
    unresolved = [token for token in form.tokens(sections) if not certificate.present(values.get(token))]
    sections = form.render_rows(sections,values)
    for section in sections:
        for row in section['rows']:
            for part in row['parts']:
                if re.fullmatch(r'LESSOR_\d+_(?:NAME|IDENTITY_NUMBER)',part.get('name','')):
                    part['editable']=False
    # Register choices are read only and scoped by the existing portfolio checks.
    tenant_choices = [] if frozen else [{key:row.get(key) for key in
        ('legal_name','identity_number','signatory_name','signatory_identity_number','signatory_capacity')}
        for row in tenant_register.rows()]

    arguments={'property_id':property_id,'draft_id':draft['id'] if draft else None}
    if request.args.get('blank')=='1':arguments['blank']='1'
    return render_pm_template('program_billing/lease_certificate_view.html',title='Data Capture',
        property=prop,lease_draft=draft,viewer_only=True,document_token='',
        unresolved=unresolved,unavailable=None,sections=sections,errors=errors,frozen=frozen,
        preview=False,owners_managed=True,lessor_count=lessor_count,lessee_count=lessee_count,
        linked_accounts=linked,bank_reason=bank_reason,
        selected_bank=str(request.form.get('bank_detail_id',certificate.bank_snapshot(items).get('bank_detail_id') or '')),
        registration_token=request.form.get('registration_token') or str(uuid.uuid4()),
        owner_choices=[{key:row.get(key) for key in ('name','identity_number')} for row in owners],tenant_choices=tenant_choices,
        certificate_edit=url_for('billing_bp.lease_certificate_edit',property_id=property_id,draft_id=draft['id'] if draft else None),
        certificate_preview=url_for('billing_bp.lease_live_view',component='certificate',**arguments)),400 if errors else 200
