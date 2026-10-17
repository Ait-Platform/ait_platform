"""Reusable, manager-scoped tenant identities. No ledger or legacy tenant side effects."""
import re
from flask import abort, request, redirect, url_for, flash, current_app
from flask_login import current_user
from sqlalchemy import text, inspect
from sqlalchemy.exc import SQLAlchemyError
from app.extensions import db
from app.program_billing.routes import billing_bp
from app.program_billing import lease_snapshot as snapshot
from app.program_billing.lease_management import render_pm_template

FIELDS = {'party_type':32,'legal_name':200,'identity_number':80,'address':2000,'email':255,'phone':50,
          'signatory_name':200,'signatory_identity_number':80,'signatory_capacity':200}


def readiness():
    inspector = inspect(db.session.connection())
    required = set(FIELDS) | {'id','manager_id','created_at'}
    if not inspector.has_table('bil_tenant_identity') or not required <= {c['name'] for c in inspector.get_columns('bil_tenant_identity')}:
        return 'Tenant registration requires the approved pm_tenant_001 schema. No migration has been run automatically.'
    return None

def rows():
    if readiness():return []
    return db.session.execute(text('SELECT * FROM bil_tenant_identity WHERE manager_id=:u ORDER BY legal_name,id'), {'u':current_user.id}).mappings().all()

def selected(identifier):
    if readiness():abort(503, description=readiness())
    try:identifier=int(identifier)
    except (ValueError, TypeError):abort(400,description='Select a saved tenant.')
    row = db.session.execute(text('SELECT * FROM bil_tenant_identity WHERE id=:id AND manager_id=:u'), {'id':identifier,'u':current_user.id}).mappings().first()
    if row is None:abort(404)
    return dict(row)

def validate(form):
    data={key:form.get(key,'').strip() for key in FIELDS}
    errors=[]
    if data['party_type'] not in ('natural_person','juristic_person'):errors.append('Choose a tenant type.')
    for key in ('legal_name','identity_number','address'):
        if not data[key]:errors.append(key.replace('_',' ').title()+' is required.')
    for key,limit in FIELDS.items():
        if len(data[key])>limit:errors.append(key.replace('_',' ').title()+' is too long.')
        if re.search(r'\[[^\[\]]+\]',data[key]):errors.append('Enter actual particulars rather than merge placeholders.')
    if data['email'] and not re.fullmatch(r'[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+',data['email']):errors.append('Enter a valid email address.')
    for key in ('signatory_name','signatory_identity_number','signatory_capacity'):
        if data['party_type']=='natural_person':data[key]=''
        elif not data[key]:errors.append('Juristic-person '+key.replace('_',' ')+' is required.')
    return data,errors

@billing_bp.route('/billing/leases/property/<int:property_id>/tenants', methods=['GET','POST'], endpoint='lease_tenants')
@snapshot.endpoint
def register(property_id):
    prop=snapshot.authorised_property(property_id, request.method=='POST')
    draft=None
    if request.values.get('draft_id'):
        snapshot.require_particulars_schema()
        draft=snapshot.draft_row(property_id,request.values['draft_id'])
    problem=readiness();errors=[];data={}
    if request.method=='POST':
        snapshot.require_particulars_schema()
        if problem:abort(503,description=problem)
        data,errors=validate(request.form)
        if not errors:
            params=dict(data,manager_id=current_user.id)
            columns=', '.join(params);values=', '.join(':'+k for k in params)
            try:
                # Matching identities are reused, never overwritten by a duplicate submission.
                result=db.session.execute(text('INSERT INTO bil_tenant_identity ('+columns+') VALUES ('+values+") ON CONFLICT (manager_id,party_type,lower(replace(identity_number,' ',''))) DO NOTHING RETURNING id"),params).scalar_one_or_none()
                if result is None:
                    result=db.session.execute(text("SELECT id FROM bil_tenant_identity WHERE manager_id=:manager_id AND party_type=:party_type AND lower(replace(identity_number,' ',''))=lower(replace(:identity_number,' ',''))"),params).scalar_one()
                db.session.commit()
            except SQLAlchemyError:
                db.session.rollback();errors.append('Tenant could not be saved. Your entered details have been retained; please retry.')
            else:
                flash('Tenant saved. Select the registered tenant in Parties.','success')
                return redirect(url_for('billing_bp.lease_particular_group',property_id=property_id,group='parties',draft_id=draft['id'] if draft else None,tenant_id=result))
    return render_pm_template('program_billing/lease_tenants.html',title='Tenant register',property=prop,lease_draft=draft,unavailable=problem or snapshot.particulars_readiness(),
        back_url=url_for('billing_bp.lease_particular_group',property_id=property_id,group='parties',draft_id=draft['id'] if draft else None),
        tenants=rows(),data=data,fields=FIELDS,errors=errors,write_enabled=current_app.config.get('PM_LEASE_PARTICULARS_WRITES_APPROVED') is True and not problem),400 if errors else 200
