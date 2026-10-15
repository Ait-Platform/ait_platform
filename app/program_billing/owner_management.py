"""Owner persistence, disabled until explicit approval and schema verification."""
from functools import wraps
from decimal import Decimal, InvalidOperation
from flask import abort, current_app, jsonify, request, flash, g
from flask_login import current_user, login_required
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.exceptions import HTTPException
from app.extensions import db
from app.program_billing.routes import billing_bp

FIELDS = {'type':16,'name':200,'identity_number':80,'email':255,'phone':50,'residential_address':2000,'postal_address':2000}


def owners_enabled():
    g.pm_owner_unavailable_reason=''
    def unavailable(reason):
        g.pm_owner_unavailable_reason=reason
        return False
    if current_app.config.get('PM_OWNER_WRITES_APPROVED') is not True:
        return unavailable('Owner saving is disabled: PM_OWNER_WRITES_APPROVED is not enabled.')
    try:
        inspector=inspect(db.session.connection())
        required={'bil_owner_identity':{'id','identity_type','display_name','created_by','identity_number','email','phone','residential_address','postal_address'},
            'bil_property_ownership':{'id','property_id','owner_id','created_by','ownership_percentage','supersedes_id','effective_from','effective_to','owner_identity_snapshot'},
            'bil_ownership_verification':{'id'},'bil_property_registration':{'id'},'alembic_version':{'version_num'}}
        for table,columns in required.items():
            if not inspector.has_table(table):return unavailable('Ownership schema is missing table '+table+'.')
            missing=columns-{c['name'] for c in inspector.get_columns(table)}
            if missing:return unavailable('Ownership schema is missing columns in '+table+': '+', '.join(sorted(missing))+'.')
        revisions=db.session.execute(text('SELECT version_num FROM alembic_version')).scalars().all()
        if 'pm_owner_002' not in revisions:return unavailable('Ownership migration pm_owner_002 is not recorded in Alembic history.')
        triggers=set(db.session.execute(text("SELECT tgname FROM pg_trigger WHERE tgrelid='bil_property_ownership'::regclass AND NOT tgisinternal AND tgenabled IN ('O','A')")).scalars().all())
        if not {'bil_property_ownership_immutable','bil_property_ownership_no_truncate','pm_ownership_validate'} <= triggers:
            return unavailable('Required ownership authorization or history triggers are missing or disabled.')
        return True
    except SQLAlchemyError:
        return unavailable('Ownership database readiness could not be verified. Please contact support.')


def guarded(function):
    @wraps(function)
    @login_required
    def wrapped(*args,**kwargs):
        if not owners_enabled():return jsonify(error=g.pm_owner_unavailable_reason),503
        try:return function(*args,**kwargs)
        except HTTPException as error:
            db.session.rollback()
            return jsonify(error=error.description),error.code
        except SQLAlchemyError as error:
            db.session.rollback()
            current_app.logger.error('PM owner operation failed error=%s',type(error).__name__)
            return jsonify(error='Owner operation failed. No changes were saved.'),503
    return wrapped


def lock_property(property_id):
    row=db.session.execute(text('SELECT id FROM bil_property WHERE id=:p AND manager_id=:u FOR UPDATE'),{'p':property_id,'u':current_user.id}).scalar_one_or_none()
    if row is None:abort(404)


def accessible_owner(owner_id):
    return db.session.execute(text("""SELECT o.* FROM bil_owner_identity o WHERE o.id=:o AND
        (o.created_by=:u OR EXISTS (SELECT 1 FROM bil_property_ownership a JOIN bil_property p ON p.id=a.property_id WHERE a.owner_id=o.id AND p.manager_id=:u)) FOR SHARE"""),{'o':owner_id,'u':current_user.id}).mappings().first()


def owner_rows(property_id, include_inactive=False, include_history=False):
    return [dict(r) for r in db.session.execute(text("""SELECT a.id AS assignment_id,a.ownership_percentage,o.id AS owner_id,
        ((a.effective_to IS NULL OR a.effective_to>CURRENT_DATE) AND (a.effective_from IS NULL OR a.effective_from<=CURRENT_DATE)) AS active,
        o.identity_type AS type,o.display_name AS name,o.identity_number,o.email,o.phone,o.residential_address,o.postal_address,o.created_by,a.effective_from,a.effective_to,a.created_at,a.supersedes_id,a.owner_identity_snapshot,
        NOT EXISTS(SELECT 1 FROM bil_property_ownership successor WHERE successor.supersedes_id=a.id) AS current_assignment
        FROM bil_property_ownership a JOIN bil_owner_identity o ON o.id=a.owner_id JOIN bil_property p ON p.id=a.property_id
        WHERE a.property_id=:p AND p.manager_id=:u AND (:history OR NOT EXISTS(SELECT 1 FROM bil_property_ownership newer WHERE newer.supersedes_id=a.id))
        AND (:inactive OR ((a.effective_to IS NULL OR a.effective_to>CURRENT_DATE) AND (a.effective_from IS NULL OR a.effective_from<=CURRENT_DATE)))
        ORDER BY a.id"""),{'p':property_id,'u':current_user.id,'inactive':include_inactive,'history':include_history}).mappings().all()]


def fields(payload):
    result={}
    for key,limit in FIELDS.items():
        value=payload.get(key,'')
        if not isinstance(value,str) or len(value)>limit:abort(400,description=f'{key.replace(chr(95),chr(32))} must be text of no more than {limit} characters.')
        result[key]=value.strip()
    if result['type'] not in ('individual','company','trust') or not result['name']:abort(400,description='Choose an owner type and enter a name.')
    if payload.get('postal_same') is True:result['postal_address']=result['residential_address']
    return result


def percentage(payload):
    value=payload.get('percentage')
    if value in (None,''):return None
    try:
        number=Decimal(str(value))
        if not number.is_finite() or number<0 or number>100 or number.as_tuple().exponent < -4:raise ValueError()
        return number
    except (InvalidOperation,ValueError):abort(400,description='Ownership percentage must be between 0 and 100 with at most four decimal places.')


@billing_bp.get('/billing/property-owner/owners',endpoint='owner_search')
@guarded
def owner_search():
    query=request.args.get('q','').strip()[:200]
    rows=db.session.execute(text("""SELECT o.id AS owner_id,o.identity_type AS type,o.display_name AS name,o.identity_number,o.email,o.phone,o.residential_address,o.postal_address
        FROM bil_owner_identity o WHERE (o.created_by=:u OR EXISTS(SELECT 1 FROM bil_property_ownership a JOIN bil_property p ON p.id=a.property_id WHERE a.owner_id=o.id AND p.manager_id=:u))
        AND (strpos(lower(o.display_name),lower(:q))>0 OR strpos(lower(coalesce(o.identity_number,'')),lower(:q))>0) ORDER BY o.display_name,o.id LIMIT 30"""),{'u':current_user.id,'q':query}).mappings().all()
    return jsonify(owners=[dict(r) for r in rows])


@billing_bp.post('/billing/property-owner/<int:property_id>/owners',endpoint='owner_assign')
@guarded
def owner_assign(property_id):
    payload=request.get_json(silent=True)
    if not isinstance(payload,dict):abort(400)
    if 'active' in payload and type(payload['active']) is not bool:abort(400)
    lock_property(property_id)
    share=percentage(payload)
    owner_id=payload.get('owner_id')
    if owner_id is not None:
        if type(owner_id) is not int or owner_id<=0:abort(400)
        if accessible_owner(owner_id) is None:abort(404)
    else:
        values=fields(payload);values['u']=current_user.id
        if not values['identity_number']:abort(400,description='An ID or registration number is required to register a new identity safely. Select an existing owner otherwise.')
        owner_id=None
        if values['identity_number']:
            db.session.execute(text("SELECT pg_advisory_xact_lock(hashtext(:scope),hashtext(lower(btrim(:identifier))))"),{'scope':'pm_owner:'+str(current_user.id)+':'+values['type'],'identifier':values['identity_number']})
            owner_id=db.session.execute(text('SELECT id FROM bil_owner_identity WHERE created_by=:u AND identity_type=:type AND lower(btrim(identity_number))=lower(btrim(:identity_number))'),values).scalar_one_or_none()
        if owner_id is None:
            owner_id=db.session.execute(text("""INSERT INTO bil_owner_identity(identity_type,display_name,identity_number,email,phone,residential_address,postal_address,created_by)
                VALUES (:type,:name,NULLIF(:identity_number,''),:email,:phone,:residential_address,:postal_address,:u) RETURNING id"""),values).scalar_one()
    existing=next((r for r in owner_rows(property_id, include_inactive=True) if r['owner_id']==owner_id),None)
    if existing is not None and not existing['active']:
        change_assignment(property_id,existing['assignment_id'],{'active':True,'percentage':payload.get('percentage')})
    if existing is None:
        db.session.execute(text('INSERT INTO bil_property_ownership(property_id,owner_id,created_by,ownership_percentage) VALUES (:p,:o,:u,:s)'),{'p':property_id,'o':owner_id,'u':current_user.id,'s':share})
    if payload.get('active') is False:
        assignment=next((r for r in owner_rows(property_id) if r['owner_id']==owner_id),None)
        if assignment:change_assignment(property_id,assignment['assignment_id'],{'active':False})
    result=owner_rows(property_id)
    db.session.commit()
    return jsonify(owners=result,property_id=property_id)


@billing_bp.patch('/billing/property-owner/owners/<int:owner_id>',endpoint='owner_edit')
@guarded
def owner_edit(owner_id):
    payload=request.get_json(silent=True)
    if not isinstance(payload,dict):abort(400)
    if 'active' in payload:
        property_id=payload.get('property_id');assignment_id=payload.get('assignment_id')
        if type(property_id) is not int or type(assignment_id) is not int:abort(400)
        lock_property(property_id)
        match=db.session.execute(text('SELECT owner_id FROM bil_property_ownership WHERE id=:a AND property_id=:p'),{'a':assignment_id,'p':property_id}).scalar_one_or_none()
        if match!=owner_id:abort(404)
    values=fields(payload);values.update(o=owner_id,u=current_user.id)
    updated=db.session.execute(text("""UPDATE bil_owner_identity SET identity_type=:type,display_name=:name,identity_number=NULLIF(:identity_number,''),email=:email,phone=:phone,residential_address=:residential_address,postal_address=:postal_address
        WHERE id=:o AND created_by=:u RETURNING id"""),values).scalar_one_or_none()
    if updated is None:abort(404)
    if 'active' in payload:change_assignment(property_id,assignment_id,{k:payload[k] for k in ('active','percentage') if k in payload})
    db.session.commit()
    flash("Owner changes saved.","success")
    return jsonify(owner_id=updated, message="Owner changes saved.")


@billing_bp.route('/billing/property-owner/<int:property_id>/assignments/<int:assignment_id>',methods=['PATCH','DELETE'],endpoint='owner_assignment_edit')
@guarded
def owner_assignment_edit(property_id,assignment_id):
    lock_property(property_id)
    old=db.session.execute(text('SELECT * FROM bil_property_ownership WHERE id=:a AND property_id=:p AND NOT EXISTS(SELECT 1 FROM bil_property_ownership n WHERE n.supersedes_id=:a)'),{'a':assignment_id,'p':property_id}).mappings().first()
    if old is None:abort(404)
    payload=request.get_json(silent=True) if request.method=='PATCH' else {'active':False}
    if not isinstance(payload,dict):abort(400)
    change_assignment(property_id,assignment_id,payload,old)
    result=owner_rows(property_id);db.session.commit()
    flash("Ownership assignment changes saved.","success")
    return jsonify(owners=result,property_id=property_id,message="Ownership assignment changes saved.")


def change_assignment(property_id,assignment_id,payload,old=None):
    if old is None:
        old=db.session.execute(text('SELECT * FROM bil_property_ownership WHERE id=:a AND property_id=:p AND NOT EXISTS(SELECT 1 FROM bil_property_ownership n WHERE n.supersedes_id=:a)'),{'a':assignment_id,'p':property_id}).mappings().first()
    if old is None:abort(409,description='This assignment changed. Reload the page and reopen this owner before saving.')
    share=percentage(payload) if 'percentage' in payload else old['ownership_percentage']
    start=old['effective_from'];end=old['effective_to']
    if 'active' in payload:
        if type(payload['active']) is not bool:abort(400)
        today=db.session.execute(text('SELECT CURRENT_DATE')).scalar_one()
        was_active=(end is None or end>today) and (start is None or start<=today)
        if payload['active']==was_active and share==old['ownership_percentage']:return
        if payload['active']:
            start=today;end=None
        else:
            end=today
            if start is not None and start>today:start=today
    db.session.execute(text('INSERT INTO bil_property_ownership(property_id,owner_id,created_by,ownership_percentage,supersedes_id,effective_from,effective_to) VALUES (:p,:o,:u,:s,:a,:start,:end)'),
        {'p':property_id,'o':old['owner_id'],'u':current_user.id,'s':share,'a':assignment_id,'start':start,'end':end})
