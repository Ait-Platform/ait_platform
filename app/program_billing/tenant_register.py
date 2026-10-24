"""bil_tenant register and its representative, saved atomically; no billing writes."""
import re,uuid
from copy import deepcopy
from flask import abort, request, redirect, url_for, flash
from flask_login import current_user
from sqlalchemy import text, inspect
from sqlalchemy.exc import SQLAlchemyError
from app.extensions import db
from app.program_billing.routes import billing_bp
from app.program_billing import lease_snapshot as snapshot
from app.program_billing.lease_management import render_pm_template

FIELDS = {'party_type':32,'legal_name':200,'identity_number':80,'address':500,'email':255,'phone':50,
          'representative_name':200,'representative_identity_number':80,'representative_phone':50,'representative_email':255}
LEASE_FIELDS = ('party_type','legal_name','identity_number','address','email','phone','signatory_name','signatory_identity_number','signatory_capacity')

def readiness():
    inspector=inspect(db.session.connection())
    required={'bil_tenant':{'id','manager_id','property_id','sectional_unit_id','party_type','name','identity_number','address','email','phone'},
              'bil_tenant_identity':{'id','tenant_id','manager_id','legal_name','identity_number','email','phone'}}
    for table,columns in required.items():
        if not inspector.has_table(table) or not columns <= {c['name'] for c in inspector.get_columns(table)}:
            return 'Tenant register requires the pm_tenant_002 compatibility migration. No database changes were run automatically.'
    return None

SELECT = """SELECT t.id,t.manager_id,t.property_id,t.sectional_unit_id,t.party_type,t.name AS legal_name,t.identity_number,t.address,t.email,t.phone,
    r.id AS representative_id,r.legal_name AS signatory_name,r.identity_number AS signatory_identity_number,
    '' AS signatory_capacity,r.phone AS representative_phone,r.email AS representative_email
    FROM bil_tenant t LEFT JOIN bil_tenant_identity r ON r.tenant_id=t.id AND r.manager_id=t.manager_id"""

def rows():
    if readiness():return []
    return db.session.execute(text(SELECT+' WHERE t.manager_id=:u ORDER BY t.name,t.id'),{'u':current_user.id}).mappings().all()

def selected(identifier):
    problem=readiness()
    if problem:abort(503,description=problem)
    try:identifier=int(identifier)
    except (ValueError,TypeError):abort(400,description='Select a saved tenant.')
    row=db.session.execute(text(SELECT+' WHERE t.id=:id AND t.manager_id=:u FOR SHARE OF t'),{'id':identifier,'u':current_user.id}).mappings().first()
    if row is None:abort(404)
    result=dict(row)
    if result['party_type']=='juristic_person' and not result['representative_id']:
        abort(409,description='The selected juristic tenant has no saved representative.')
    if result['party_type']=='natural_person':
        for key in ('signatory_name','signatory_identity_number','signatory_capacity','representative_phone','representative_email'):result[key]=''
        result['representative_id']=None
    return result

def validate(form):
    data={key:form.get(key,'').strip() for key in FIELDS};errors=[]
    if data['party_type'] not in ('natural_person','juristic_person'):errors.append('Choose a tenant type.')
    if data['party_type']=='natural_person':
        for key in FIELDS:
            if key.startswith('representative_'):data[key]=''
    required=['legal_name','identity_number','address']
    if data['party_type']=='juristic_person':required += [k for k in FIELDS if k.startswith('representative_')]
    for key in required:
        if not data[key]:errors.append(key.replace('_',' ').title()+' is required.')
    for key,limit in FIELDS.items():
        if len(data[key])>limit:errors.append(key.replace('_',' ').title()+' is too long.')
        if re.search(r'\[[^\[\]]+\]',data[key]):errors.append('Enter actual particulars rather than merge placeholders.')
    for key in ('email','representative_email'):
        if data[key] and not re.fullmatch(r'[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+',data[key]):errors.append('Enter a valid '+key.replace('_',' ')+'.')
    return data,errors

def assignment(form, context_property_id):
    result={'property_id':None,'sectional_unit_id':None}
    for key in result:
        raw=form.get(key,'').strip()
        if raw:
            try:result[key]=int(raw)
            except ValueError:abort(400,description='Choose a valid property and unit.')
            if result[key]<=0:abort(400,description='Choose a valid property and unit.')
    if result['property_id'] is not None:
        snapshot.authorised_property(result['property_id'],True)
        if result['property_id']!=context_property_id:abort(400,description='Choose the selected lease property or leave the tenant unassigned.')
    if result['sectional_unit_id'] is not None:
        unit_property=db.session.execute(text('SELECT property_id FROM bil_sectional_unit WHERE id=:id FOR SHARE'),{'id':result['sectional_unit_id']}).scalar_one_or_none()
        if unit_property is None or unit_property!=result['property_id']:
            abort(400,description='The unit must belong to the selected property.')
    return result

def units(property_id):
    inspector=inspect(db.session.connection())
    if not inspector.has_table('bil_sectional_unit'):return []
    columns={c['name'] for c in inspector.get_columns('bil_sectional_unit')}
    if not {'id','name','property_id'}<=columns:return []
    return db.session.execute(text('SELECT u.id,u.name FROM bil_sectional_unit u JOIN bil_property p ON p.id=u.property_id WHERE p.id=:p AND p.manager_id=:u AND p.is_archived=false ORDER BY u.name,u.id'),{'p':property_id,'u':current_user.id}).mappings().all()

def save(data):
    params={k:data[k] for k in ('party_type','identity_number','address','email','phone')}
    params.update(name=data['legal_name'],manager_id=current_user.id,property_id=None,sectional_unit_id=None)
    # Explicit operational defaults only when those legacy columns exist. No units,
    # accounts, leases, meters or debtor records are created.
    columns={c['name'] for c in inspect(db.session.connection()).get_columns('bil_tenant')}
    for key,value in {'rent_includes_metro':0,'email_statements':False,'is_active':True}.items():
        if key in columns:params[key]=value
    statement='INSERT INTO bil_tenant ('+', '.join(params)+') VALUES ('+', '.join(':'+k for k in params)+") ON CONFLICT (manager_id,party_type,lower(replace(identity_number,' ',''))) DO NOTHING RETURNING id"
    tenant_id=db.session.execute(text(statement),params).scalar_one_or_none()
    if tenant_id is None:
        tenant_id=db.session.execute(text("SELECT id FROM bil_tenant WHERE manager_id=:manager_id AND party_type=:party_type AND lower(replace(identity_number,' ',''))=lower(replace(:identity_number,' ','')) FOR UPDATE"),params).scalar_one()
    if data['party_type']=='juristic_person':
        db.session.execute(text("""INSERT INTO bil_tenant_identity (tenant_id,manager_id,legal_name,identity_number,phone,email)
            SELECT :t,:u,:n,:i,:p,:e WHERE NOT EXISTS (SELECT 1 FROM bil_tenant_identity WHERE tenant_id=:t AND manager_id=:u)"""),{'t':tenant_id,'u':current_user.id,'n':data['representative_name'],
                'i':data['representative_identity_number'],'p':data['representative_phone'],'e':data['representative_email']})
    return tenant_id

@billing_bp.route('/billing/leases/property/<int:property_id>/tenants/select',methods=['GET','POST'],endpoint='lease_select_tenants')
@billing_bp.route('/billing/leases/property/<int:property_id>/tenants',methods=['GET','POST'],endpoint='lease_tenants')
@snapshot.endpoint
def register(property_id):
    selecting=request.endpoint=='billing_bp.lease_select_tenants'
    prop=snapshot.authorised_property(property_id,request.method=='POST');draft=None
    if request.values.get('draft_id'):
        snapshot.require_particulars_schema();draft=snapshot.draft_row(property_id,request.values['draft_id'],request.method=='POST')
    problem=readiness();errors=[];data={}
    from app.program_billing import lease_workflow as workflow
    if request.method=='POST' and selecting:
        if request.form.get('action')!='select_tenants':abort(400,description='Use the tenant selection form.')
        snapshot.require_particulars_schema()
        if problem:abort(503,description=problem)
        ids=request.form.getlist('tenant_ids')
        if not ids:abort(400,description='Select at least one registered tenant.')
        records=[selected(identifier) for identifier in ids]
        if len({r['id'] for r in records})!=len(records):abort(400,description='Select each tenant only once.')
        items=deepcopy(draft['items']) if draft else {}
        workflow.set_selected_tenants(items,records)
        if draft:workflow.save_items(draft,items)
        else:draft=workflow.create_parties_draft(property_id,items)
        flash('Lease particulars saved successfully. Lease remains a draft.','success')
        return redirect(url_for('billing_bp.lease_particular_group',property_id=property_id,group='parties',draft_id=draft['id']))
    if request.method=='POST':
        if request.form.get('action')=='select_tenants':abort(400,description='Use Select Tenant to change lease selections.')
        if problem:abort(503,description=problem)
        data,errors=validate(request.form)
        if not errors:
            try:
                tenant_id=save(data);db.session.commit()
            except SQLAlchemyError:
                db.session.rollback();errors.append('Tenant and representative could not be saved. No partial save was confirmed; entered details are retained.')
            else:
                flash('Tenant saved. Use Select Tenant to add it to the lease.','success')
                return redirect(url_for('billing_bp.lease_particular_group',property_id=property_id,group='parties',draft_id=draft['id'] if draft else None))
    return render_pm_template('program_billing/lease_tenant_selection.html' if selecting else 'program_billing/lease_tenants.html',title='Select Tenant' if selecting else 'Add Tenant',property=prop,lease_draft=draft,unavailable=problem,
        back_url=url_for('billing_bp.lease_particular_group',property_id=property_id,group='parties',draft_id=draft['id'] if draft else None),
        tenants=rows(),data=data,fields=FIELDS,errors=errors,write_enabled=not problem,
        selected_tenant_ids=[str(p['tenant_id']) for p in workflow.selected_party_snapshots(draft['items'])] if draft else [],
        draft_registration_token=str(draft['registration_token']) if draft else request.values.get('registration_token') or str(uuid.uuid4()),
        first_save_available=selecting and not draft and not problem,
        first_save_message='Save selected tenants to create the lease draft, then return to Part A.'),400 if errors else 200
