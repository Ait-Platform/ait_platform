"""Scoped approved-certificate preview integration. Imported by lease_preview routes."""
from copy import deepcopy
from io import BytesIO
from flask import abort,request,url_for,current_app,send_file,redirect,flash,jsonify
from datetime import datetime,timezone
import uuid,re
from flask_login import current_user
from . import lease_certificate_form as form
from . import lease_supplementary_conditions as supplementary
from app.extensions import db
from sqlalchemy.exc import IntegrityError
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
    # Keep legacy Certificate PDF links on the same protected renderer as View Lease.
    return redirect(url_for('billing_bp.lease_kit_pdf',property_id=property_id,
        draft_id=request.args.get('draft_id'),component='certificate'))


def word(property_id):
    _,_,output,_=payload(property_id)
    return send_file(BytesIO(output),mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        as_attachment=True,download_name='AIT-Lease-Certificate-DRAFT.docx',conditional=False,etag=False,max_age=0)


def selector(property_id):
    return edit(property_id,'parties')


def edit(property_id,section_key):
    from app.program_billing.lease_management import render_pm_template
    from app.program_billing import lease_workflow as workflow,lease_property_payments as payments,tenant_register
    from .lease_kit import developing_items
    definition=form.capture_section(section_key)
    if definition is None:abort(404)
    prop,draft,saved,owners,tenants=context(property_id,allow_blank=False)
    items=developing_items(saved,owners)
    frozen=(payments.signed(draft) if draft else False) or any(
        generation.get('signature_or_issuance_approved') is True for generation in
        saved.get('_generations',[])+saved.get('_ait_standard',{}).get('generations',[]))
    bank_reason=payments.readiness() if section_key=='banking' else None
    linked=payments.existing_property_accounts(property_id) if section_key=='banking' and not bank_reason else []
    values=certificate.values(items,None if frozen else dict(prop),owners,tenants)
    if section_key=='parties':
        for index,party in enumerate(certificate.selected_tenants(items),1):
            registered=next((row for row in tenants if str(row['id'])==str(party.get('tenant_id'))),{})
            for key in ('address','email','phone'):
                values['[LESSEE_'+str(index)+'_'+key.upper()+']']=party.get(key,registered.get(key)) or ''
    lessor_count,lessee_count=form.counts(items)
    errors=[];date_differences=[];calculated_dates={}
    if section_key=='period' and not frozen:
        start=request.values.get('COMMENCEMENT_DATE',values.get('[COMMENCEMENT_DATE]',''))
        duration=request.values.get('DURATION_MONTHS',values.get('[DURATION_MONTHS]',''))
        if start and duration:
            calculated_dates,period_errors=workflow.calculated_period({'s1-18':start,'s1-17':duration},saved)
            if request.method=='POST':errors.extend(period_errors)
            for key,label in (('expiry_date','Expiry Date'),('renewal_notice_date','Renewal Notice Date')):
                old=values.get('['+key.upper()+']','');new=calculated_dates.get(key)
                if old and new and old!=new:date_differences.append({'label':label,'saved':old,'calculated':new})
                legacy=saved.get('s1-20',{}).get('value') if key=='expiry_date' else None
                if legacy and new and legacy!=new and legacy!=old:
                    date_differences.append({'label':'Legacy Expiry Date','saved':legacy,'calculated':new})
    if section_key=='period' and request.method=='GET' and request.args.get('calculate')=='1':
        return jsonify(dates=calculated_dates,differences=date_differences)
    if request.method=='POST':
        snapshot.require_particulars_schema()
        if frozen:abort(409,description='Issued or signed lease particulars are preserved. Use Addendum for changes.')
        try:
            if draft and str(draft['version'])!=request.form.get('version'):
                abort(409,description='The draft changed. Reload before saving.')
            if request.form.get('action','save')!='save':abort(400)
            if section_key=='parties':
                _,lessee_count=form.form_counts(request.form)
            sections=[] if section_key=='additional' else form.capture_layout(section_key,lessor_count,lessee_count)
            submitted_form=request.form.copy()
            for key in ('EXPIRY_DATE','RENEWAL_NOTICE_DATE'):
                if section_key=='period':submitted_form.pop(key,None)
            edits=form.submitted(submitted_form,sections)
            if section_key=='period':
                if errors:raise ValueError('Review the lease period inputs.')
                if date_differences and request.form.get('confirm_period_dates')!='1':
                    raise ValueError('Saved dates differ from the calculated dates. Confirm the replacements before saving.')
                for key in ('expiry_date','renewal_notice_date'):
                    if key in calculated_dates:edits['['+key.upper()+']']=calculated_dates[key]
            edits={token:value for token,value in edits.items() if token!='[LEASE_REFERENCE]' and not token.endswith('_AUTHORITY_EVIDENCE]') and not re.fullmatch(r'\[LESSOR_\d+_(?:NAME|IDENTITY_NUMBER)\]',token)}
            selected_register={}
            if section_key=='parties':
                original_tenants=certificate.selected_tenants(items)
                for index in range(1,lessee_count+1):
                    identifier=request.form.get('tenant_register_id_'+str(index),'')
                    if not identifier:continue
                    registered=tenant_register.selected(identifier)
                    selected_register[index]=registered
                    old=original_tenants[index-1] if index<=len(original_tenants) else {}
                    if str(old.get('tenant_id',''))!=str(registered['id']):
                        for key in ('legal_name','identity_number','signatory_name','signatory_identity_number'):
                            edits['[LESSEE_'+str(index)+'_'+key.upper()+']']=registered.get(key) or ''
            if section_key=='additional':
                pending=deepcopy(saved)
                sc_data=supplementary.submitted(request.form,saved)
                if sc_data is not None:pending[supplementary.KEY]=sc_data
            else:pending=form.merge_section(saved,items,edits,lessor_count,lessee_count,section_key)
            if section_key=='parties':
                for index in range(1,lessee_count+1):
                    party_type=request.form.get('LESSEE_'+str(index)+'_PARTY_TYPE','')
                    if party_type:
                        if party_type not in ('natural_person','juristic_person'):raise ValueError('Choose a valid Lessee type.')
                        pending['_parties']['lessees'][index-1]['party_type']=party_type
                for index,registered in selected_register.items():
                    row=pending['_parties']['lessees'][index-1]
                    if all(str(row.get(key) or '')==str(registered.get(key) or '') for key in ('legal_name','identity_number')):
                        row['tenant_id']=registered['id']
                        if not request.form.get('LESSEE_'+str(index)+'_PARTY_TYPE'):
                            row['party_type']=registered.get('party_type') or ''
                        for key in ('address','email','phone'):
                            if '[LESSEE_'+str(index)+'_'+key.upper()+']' not in edits:row[key]=registered.get(key) or ''
                if pending.get('_parties',{}).get('lessees'):
                    pending['_parties']['lessee']=deepcopy(pending['_parties']['lessees'][0])
                original_rep=items.get('_parties',{}).get('lessor',{})
                proposed_rep=pending.get('_parties',{}).get('lessor',{})
                if any(original_rep.get(key,'')!=proposed_rep.get(key,'') for key in
                       ('signatory_name','signatory_identity_number','signatory_capacity')):
                    proposed_rep['capacity_confirmed']=False
                if original_rep.get('signatory_identity_number','')!=proposed_rep.get('signatory_identity_number',''):
                    proposed_rep.pop('owner_id',None)
                    if 'identity_source' in proposed_rep:proposed_rep['identity_source']='lease_particulars'
            if section_key=='banking':
                saved_id=str(certificate.bank_snapshot(saved).get('bank_detail_id') or '')
                bank_id=request.form.get('bank_detail_id',saved_id)
                old_bank=certificate.bank_snapshot(saved)
                id_only=bank_id and not any(certificate.present(old_bank.get(key)) for key in certificate.BANK_FIELDS)
                explicitly_selected=request.form.get('refresh_bank_snapshot')=='1'
                if bank_id!=saved_id or id_only or explicitly_selected:
                    if bank_reason:raise ValueError('Bank selection is unavailable. The saved snapshot has not changed.')
                    account=next((row for row in linked if str(row['bank_detail_id'])==bank_id),None)
                    if bank_id and account is None:raise ValueError('Choose an account already linked to this property.')
                    if account:
                        pending['_certificate_banking']={key:account[key] for key in certificate.BANK_FIELDS}
                        pending['_certificate_banking'].update(schema_version=1,bank_detail_id=account['bank_detail_id'],
                            property_id=property_id,selected_by=int(current_user.id),selected_at=datetime.now(timezone.utc).isoformat())
                    else:pending['_certificate_banking']={'schema_version':1,'property_id':property_id,'bank_detail_id':None}
            if draft:
                if section_key=='parties' and not saved.get('s1-01',{}).get('value'):
                    pending['s1-01']={'value':'LEASE-'+str(draft['id'])}
                elif section_key=='parties':pending['s1-01']=deepcopy(saved['s1-01'])
                workflow.save_items(draft,pending)
            else:draft=workflow.create_parties_draft(property_id,pending,automatic_reference=True)
            flash(definition[1]+' saved.','success')
            if request.form.get('capture_return')=='section' and form.capture_section(request.form.get('capture_return_section')):
                return redirect(url_for('billing_bp.lease_capture_section',property_id=property_id,section_key=request.form['capture_return_section'],draft_id=draft['id']))
            if request.form.get('capture_return')=='back':
                return redirect(url_for('billing_bp.pm_lease_management',property_id=property_id,draft_id=draft['id']))
            return redirect(url_for('billing_bp.lease_capture_section',property_id=property_id,section_key=section_key,draft_id=draft['id']))
        except IntegrityError as error:
            db.session.rollback();payments.log_banking_integrity(error,'apply' if section_key=='banking' else 'add_account')
            errors.append('Bank account could not be saved. Its details and existing lease selections were not changed.')
        except ValueError as error:
            if section_key=='banking':
                db.session.rollback()
            errors.append(str(error))
            lessor_count=max(1,min(form.MAX_PARTIES,lessor_count))
            lessee_count=max(1,min(form.MAX_PARTIES,lessee_count))
            for token in form.tokens(form.capture_layout(section_key,lessor_count,lessee_count)):
                if not form.readonly(token) and not re.fullmatch(r'\[LESSOR_\d+_(?:NAME|IDENTITY_NUMBER)\]',token) and token[1:-1] in request.form:
                    values[token]=request.form[token[1:-1]]
    if section_key=='period' and not frozen:
        for key in ('expiry_date','renewal_notice_date'):
            if key in calculated_dates:values['['+key.upper()+']']=calculated_dates[key]
    section=form.render_rows(form.capture_layout(section_key,lessor_count,lessee_count),values)[0]
    for row in section['rows']:
        for part in row['parts']:
            if section_key=='period' and part.get('name') in ('EXPIRY_DATE','RENEWAL_NOTICE_DATE'):part['editable']=False
            if re.fullmatch(r'LESSOR_\d+_(?:NAME|IDENTITY_NUMBER)',part.get('name','')):part['editable']=False
    if section_key=='parties':
        section['rows']=[dict(row,parts=[part for part in row['parts'] if part.get('name')!='LEASE_REFERENCE'
            and not part.get('name','').endswith(('_AUTHORITY_CONFIRMED','_AUTHORITY_EVIDENCE'))])
            for row in section['rows']]
        section['rows']=[row for row in section['rows'] if any('token' in part for part in row['parts'])]
        for row in section['rows']:
            if row['label']=='Capacity / authority':row['label']='Representative capacity'
    if section_key=='banking':
        # Show each saved account field once; the document layout repeats some of them.
        seen=set();summary=[]
        for row in section['rows']:
            parts=[]
            for part in row['parts']:
                if 'name' in part and part['name'] not in seen:
                    seen.add(part['name']);parts.append(part)
            if parts:summary.append(dict(row,parts=parts))
        section['rows']=summary
    party_rows=[];lessee_groups=[];lessee_template=[]
    if section_key=='parties':
        for row in section['rows']:
            name=next((part['name'] for part in row['parts'] if 'name' in part),'')
            if name.startswith('LESSEE_'):
                index=int(name.split('_')[1])
                while len(lessee_groups)<index:lessee_groups.append([])
                lessee_groups[index-1].append(row)
            else:party_rows.append(row)
        prototype=form.render_rows(form.capture_layout('parties',lessor_count,1),{})[0]
        lessee_template=[dict(row,parts=[part for part in row['parts'] if not part.get('name','').endswith(('_AUTHORITY_CONFIRMED','_AUTHORITY_EVIDENCE'))])
            for row in prototype['rows'] if any(part.get('name','').startswith('LESSEE_1_') for part in row['parts'])]
        for row in lessee_template:
            if row['label']=='Capacity / authority':row['label']='Representative capacity'
    tenant_choices=[] if frozen or section_key!='parties' else [
        {key:row.get(key) for key in ('id','party_type','legal_name','identity_number','address','email','phone','signatory_name','signatory_identity_number','signatory_capacity','representative_phone','representative_email')}
        for row in tenant_register.rows()]
    saved_bank=str(certificate.bank_snapshot(saved).get('bank_detail_id') or '')
    requested_bank=request.form.get('bank_detail_id',saved_bank)
    selectable={str(row['bank_detail_id']) for row in linked}
    selected_bank=requested_bank if not bank_reason and (requested_bank in selectable or requested_bank in ('',saved_bank)) else saved_bank
    historical_financial=[]
    if section_key=='financial':
        for row in form.render_rows([form.layout(lessor_count,lessee_count)[3]],values)[0]['rows']:
            parts=[part for part in row['parts'] if part.get('value') and part.get('name') not in ('MONTHLY_RENT','DEPOSIT_AMOUNT')]
            if parts:historical_financial.append(dict(row,parts=parts))
    lessee_details=[]
    for party in certificate.selected_tenants(saved):
        registered=next((row for row in tenants if str(row['id'])==str(party.get('tenant_id'))),{})
        detail={key:registered.get(key) for key in ('party_type','legal_name','identity_number','address','email','phone','signatory_name','signatory_identity_number','signatory_capacity','representative_phone','representative_email')}
        detail.update({key:value for key,value in party.items() if certificate.present(value)})
        if any(certificate.present(detail.get(key)) for key in ('legal_name','identity_number','signatory_name')):lessee_details.append(detail)
    legacy_additional=[];legacy_sc=''
    if section_key=='additional':
        from .lease_standard import special_conditions
        legacy_sc=special_conditions(saved)
        for row in section['rows']:
            parts=[part for part in row['parts'] if part.get('value')]
            if parts:legacy_additional.append(dict(row,parts=parts))
    representative=items.get('_parties',{}).get('lessor',{})
    matches=[row for row in owners if representative.get('owner_id') is not None and str(row.get('owner_id'))==str(representative['owner_id'])]
    if not matches and representative.get('signatory_identity_number'):
        matches=[row for row in owners if row.get('identity_number')==representative['signatory_identity_number']]
    representative_owner=matches[0] if len(matches)==1 else None
    saved_lessees=certificate.selected_tenants(items)
    lessee_types=[request.form.get('LESSEE_'+str(index+1)+'_PARTY_TYPE',saved_lessees[index].get('party_type','') if index<len(saved_lessees) else '') for index in range(lessee_count)]
    party_validation=[]
    if section_key=='parties':
        from .lease_certificate_parties import blocks
        for group in blocks(saved,owners,tenants):
            for party in group['parties']:
                party_validation.extend(group['label']+' '+(party['name'] or party['identity_number'])+': '+warning for warning in party['warnings'])
    arguments={'property_id':property_id,'draft_id':draft['id'] if draft else None}
    return render_pm_template('program_billing/lease_capture_section.html',title=definition[1],property=prop,
        lease_draft=draft,section_key=section_key,section=section,errors=errors,frozen=frozen,
        party_rows=party_rows,lessee_groups=lessee_groups,lessee_template=lessee_template,party_validation=party_validation,
        lessee_types=lessee_types,
        lessee_details=lessee_details,sc_headings=supplementary.HEADINGS,legacy_additional=legacy_additional,
        sc_editor=supplementary.editor(saved,request.form if errors and section_key=='additional' else None),legacy_sc=legacy_sc,
        lessor_count=lessor_count,lessee_count=lessee_count,tenant_choices=tenant_choices,
        bank_reason=bank_reason,linked_accounts=linked,
        selected_bank=selected_bank,historical_financial=historical_financial,
        saved_bank_snapshot=certificate.bank_snapshot(saved),
        date_differences=date_differences,calculated_dates=calculated_dates,
        owner_information=owners,owner_representative=representative_owner,
        missing_bank_fields=[part['name'].replace('_',' ').title() for row in section['rows'] for part in row['parts']
            if section_key=='banking' and part.get('name') and not part.get('value')],
        selected_tenant_ids=[str(row.get('tenant_id') or '') for row in certificate.selected_tenants(items)],
        bank_choices=[{key:account.get(key) for key in ('bank_detail_id',*certificate.BANK_FIELDS)} for account in linked],
        registration_token=request.form.get('registration_token') or str(uuid.uuid4()),
        capture_sections=[{'key':key,'label':label.split('. ',1)[-1],
            'url':url_for('billing_bp.lease_capture_section',section_key=key,**arguments)} for key,label,_,_ in form.CAPTURE_SECTIONS],
        lease_reference=saved.get('s1-01',{}).get('value') or ('LEASE-'+str(draft['id']) if draft else 'Generated on first save'),
        capture_back=url_for('billing_bp.pm_lease_management',**arguments)),400 if errors else 200
