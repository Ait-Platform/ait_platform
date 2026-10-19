"""Authorized reusable property accounts/utilities, with explicit draft snapshots."""
from copy import deepcopy
from datetime import datetime,timezone
import json,re,uuid
from flask import abort,request,redirect,url_for,flash
from flask_login import current_user
from sqlalchemy import text,inspect
from app.extensions import db
from app.program_billing.routes import billing_bp
from app.program_billing import lease_snapshot as snapshot,lease_workflow as workflow
from app.program_billing.lease_management import render_pm_template

REVISION='pm_lease_payments_001'
BANK_FIELDS=('bank_name','account_holder','account_number','account_type','branch_code')


def readiness():
    inspector=inspect(db.session.connection())
    required={'bil_property_payment_account':{'id','property_id','bank_detail_id','for_rental','for_utilities','created_by','created_at'},
        'bil_property_utility_arrangement':{'id','property_id','version','details','created_by','created_at'},
        'bil_bank_detail':{'id',*BANK_FIELDS}}
    for name,columns in required.items():
        if not inspector.has_table(name) or not columns<={c['name'] for c in inspector.get_columns(name)}:
            return 'Reusable property banking and utilities require the reviewed pm_lease_payments_001 migration. No migration was run automatically.'
    if not inspector.has_table('alembic_version') or REVISION not in db.session.execute(text('SELECT version_num FROM alembic_version')).scalars():
        return 'Property payment migration pm_lease_payments_001 is not recorded.'
    return None


def accounts(property_id):
    return [dict(r) for r in db.session.execute(text('''SELECT a.*,b.bank_name,b.account_holder,b.account_number,b.account_type,b.branch_code
        FROM bil_property_payment_account a JOIN bil_bank_detail b ON b.id=a.bank_detail_id
        WHERE a.property_id=:p ORDER BY a.created_at,a.id'''),{'p':property_id}).mappings()]


def arrangements(property_id):
    return [dict(r) for r in db.session.execute(text('SELECT * FROM bil_property_utility_arrangement WHERE property_id=:p ORDER BY version DESC'),{'p':property_id}).mappings()]


def utility_record(property_id,identifier):
    if readiness():abort(503,description=readiness())
    try:identifier=uuid.UUID(str(identifier))
    except (ValueError,TypeError):abort(404)
    row=db.session.execute(text('SELECT * FROM bil_property_utility_arrangement WHERE property_id=:p AND id=:id'),{'p':property_id,'id':identifier}).mappings().first()
    if row is None:abort(404)
    return dict(row)


def account_input(form):
    values={k:form.get(k,'').strip() for k in BANK_FIELDS}
    limits={'bank_name':100,'account_holder':150,'account_number':100,'account_type':50,'branch_code':50}
    for key,value in values.items():
        if not value or len(value)>limits[key] or any(ord(c)<32 for c in value):raise ValueError('Enter valid '+key.replace('_',' ')+'.')
    if not re.fullmatch(r'[0-9]{6,20}',values['account_number']):raise ValueError('Account number must contain 6 to 20 digits.')
    if not re.fullmatch(r'[0-9]{6}',values['branch_code']):raise ValueError('Branch code must contain six digits.')
    if values['account_type'] not in ('current','savings','transmission','other'):raise ValueError('Select an account type.')
    values.update(for_rental=form.get('for_rental')=='on',for_utilities=form.get('for_utilities')=='on')
    if not (values['for_rental'] or values['for_utilities']):raise ValueError('Allocate the account to Rental, Utilities or both.')
    return values


def add_account(property_id,form):
    values=account_input(form)
    # Existing property lock serializes duplicate registration and reassignment.
    for row in accounts(property_id):
        if row['account_number']==values['account_number'] and row['branch_code']==values['branch_code'] and row['bank_name'].casefold()==values['bank_name'].casefold():
            if any(row[k]!=values[k] for k in (*BANK_FIELDS,'for_rental','for_utilities')):
                raise ValueError('This account is already registered with different details/allocation; it was not overwritten.')
            return
    bank_id=None
    if 'bank_detail_id' in {c['name'] for c in inspect(db.session.connection()).get_columns('bil_property')}:
        legacy=db.session.execute(text('''SELECT b.* FROM bil_bank_detail b JOIN bil_property p ON p.bank_detail_id=b.id
            WHERE p.id=:p AND p.manager_id=:u'''),{'p':property_id,'u':current_user.id}).mappings().first()
        if legacy and all(legacy[k]==values[k] for k in BANK_FIELDS):bank_id=legacy['id']
    if bank_id is None:bank_id=db.session.execute(text('''INSERT INTO bil_bank_detail (bank_name,account_holder,account_number,account_type,branch_code)
        VALUES (:bank_name,:account_holder,:account_number,:account_type,:branch_code) RETURNING id'''),values).scalar_one()
    db.session.execute(text('''INSERT INTO bil_property_payment_account(id,property_id,bank_detail_id,for_rental,for_utilities,created_by)
        VALUES (:id,:p,:b,:for_rental,:for_utilities,:u)'''),dict(values,id=uuid.uuid4(),p=property_id,b=bank_id,u=int(current_user.id)))


def add_utilities(property_id,details):
    if not isinstance(details,dict) or not str(details.get('utility_allocation_method','')).strip():raise ValueError('Enter a property utility arrangement.')
    if len(json.dumps(details))>16000:raise ValueError('Utility arrangement is too long.')
    previous=arrangements(property_id)
    if previous and previous[0]['details']==details:return
    db.session.execute(text('''INSERT INTO bil_property_utility_arrangement(id,property_id,version,details,created_by)
        VALUES (:id,:p,:v,CAST(:details AS jsonb),:u)'''),{'id':uuid.uuid4(),'p':property_id,'v':previous[0]['version']+1 if previous else 1,'details':json.dumps(details),'u':current_user.id})


def signed(draft):
    if any(e.get('kind')=='signed_original' for e in draft['items'].get('_lease_history',{}).get('documents',[])):return True
    return db.session.execute(text("SELECT 1 FROM bil_lease_document WHERE draft_id=:d AND property_id=:p AND item_key='history-signed_original' LIMIT 1"),{'d':draft['id'],'p':draft['property_id']}).first() is not None


def apply_selection(property_id,draft,form):
    if not draft:abort(400,description='Select a saved lease draft first.')
    if signed(draft):abort(409,description='A signed original is recorded. Change payment instructions or utilities through Addendum; the original snapshot was preserved.')
    available={str(a['id']):a for a in accounts(property_id)}
    selected={}
    for purpose in ('rental','utilities'):
        identifier=form.get(purpose+'_account_id','')
        if not identifier:continue
        account=available.get(identifier)
        if account is None or not account['for_'+purpose]:abort(404)
        selected[purpose]={k:account[k] for k in ('id','bank_detail_id',*BANK_FIELDS)}
        selected[purpose]['id']=str(selected[purpose]['id'])
    if not selected.get('rental'):raise ValueError('Select a Rental account.')
    utility=utility_record(property_id,form.get('utility_arrangement_id'))
    items=deepcopy(draft['items']);existing=deepcopy(items.get('_banking_utilities',{}))
    state=dict(existing,**utility['details'],**{k:selected['rental'][k] for k in BANK_FIELDS})
    state.update(schema_version=2,master_sha256=workflow.PARTIES_MASTER_SHA256,property_accounts=selected,
        utility_arrangement_id=str(utility['id']),utility_arrangement_version=utility['version'],property_id=property_id,
        selected_by=int(current_user.id),selected_at=datetime.now(timezone.utc).isoformat())
    # Confirmation flags are historical only. They are not set or asserted by saving.
    items['_banking_utilities']=state
    merged=workflow.banking_utilities_placeholder_values(items)
    items['s1-36']={'value':merged['[SHARED UTILITY ALLOCATION METHOD]']}
    items['s1-38']={'value':merged[workflow.BANKING_TOKEN]}
    workflow.save_items(draft,items)


def snapshot_errors(items):
    saved=items.get('_banking_utilities',{});errors=[]
    accounts=saved.get('property_accounts',{})
    if not accounts.get('rental'):errors.append('Select a property Rental account in Banking Details.')
    for purpose,account in accounts.items():
        try:account_input(dict(account,**{'for_'+purpose:'on'}))
        except ValueError:errors.append('Review the saved '+purpose+' account details.')
    if not saved.get('utility_allocation_method'):errors.append('Select a saved property utility arrangement in Banking Details.')
    return errors


@billing_bp.route('/billing/leases/property/<int:property_id>/banking',methods=['GET','POST'],endpoint='lease_property_banking')
@snapshot.endpoint
def workspace(property_id):
    prop,lease_reason,draft=workflow.selected_context(property_id,request.method=='POST')
    reason=readiness();errors=[]
    if request.method=='POST':
        if reason:abort(503,description=reason)
        try:
            action=request.form.get('action')
            if action=='add_account':add_account(property_id,request.form);db.session.commit();flash('Property bank account saved. Existing leases are unchanged.','success')
            elif action in ('save_utilities','reuse_utilities'):
                if action=='reuse_utilities':
                    if not draft:raise ValueError('Select a draft containing the existing utilities first.')
                    saved=draft['items'].get('_banking_utilities',{})
                    details={k:deepcopy(v) for k,v in saved.items() if k.startswith('utility_')}
                elif request.form.get('utility_mode')=='no_charge':
                    details={'utility_mode':'no_charge','utility_allocation_method':'No utilities are charged to the tenant.','utility_reference_mode':'none','utility_references':'','utility_no_references_reason':'No tenant utility charges apply.'}
                elif request.form.get('utility_mode')=='custom':
                    details={'utility_mode':'custom','utility_allocation_method':request.form.get('utility_allocation_method',''),'utility_reference_mode':'provided' if request.form.get('utility_references','').strip() else 'none','utility_references':request.form.get('utility_references',''),'utility_no_references_reason':'' if request.form.get('utility_references','').strip() else 'No separate references supplied in this arrangement.'}
                else:raise ValueError('Choose No tenant utility charges or a property-specific arrangement.')
                add_utilities(property_id,details);db.session.commit();flash('Property utility arrangement saved as a reusable version. Lease snapshots are unchanged.','success')
            elif action=='apply':
                if lease_reason:abort(503,description=lease_reason)
                apply_selection(property_id,draft,request.form);flash('Selected banking and utilities saved to this draft.','success')
            else:abort(400)
            return redirect(url_for('billing_bp.lease_property_banking',property_id=property_id,draft_id=draft['id'] if draft else None))
        except ValueError as error:db.session.rollback();errors.append(str(error))
    return render_pm_template('program_billing/lease_property_banking.html',title='Banking Details',property=prop,lease_draft=draft,
        unavailable=reason or lease_reason,errors=errors,accounts=[] if reason else accounts(property_id),arrangements=[] if reason else arrangements(property_id),
        saved_banking=draft['items'].get('_banking_utilities',{}) if draft else {},signed_lease=signed(draft) if draft else False),400 if errors else 200
