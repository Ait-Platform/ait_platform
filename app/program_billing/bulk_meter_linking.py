"""Property-only bulk links using the existing meter parent foreign key."""
from flask import request,render_template,redirect,url_for,flash,abort,current_app
from sqlalchemy import text
from werkzeug.exceptions import HTTPException
from app.extensions import db
from .metsoa_setup_safety import property_scope

def kind(value):
    value=(value or '').lower()
    return 'electricity' if value=='electrical' else value

def save(p,form):
    property_scope(p,lock=True)
    from .utilities_capture import ready,audit
    ready('Bulk Meter Linking')
    rows=[dict(r) for r in db.session.execute(text('SELECT id,meter_number,utility_type,parent_meter_id FROM bil_meter WHERE property_id=:p ORDER BY id FOR UPDATE'),dict(p=p)).mappings()]
    meters={r['id']:r for r in rows}
    try:parent=int(form.get('parent_id',''));children=[int(v) for v in form.getlist('children')]
    except (ValueError,TypeError):abort(400,description='Select registered meters.')
    utility=form.get('utility')
    if utility not in ('water','electricity') or parent not in meters:abort(400,description='Select a bulk meter registered to this property.')
    if len(children)!=len(set(children)):abort(400,description='Duplicate individual meter selection.')
    if parent in children:abort(400,description='A meter cannot supply itself.')
    if kind(meters[parent]['utility_type'])!=utility:abort(400,description='Select meters of the same utility type.')
    for child in children:
        if child not in meters:abort(400,description='All linked meters must belong to this property.')
        if kind(meters[child]['utility_type'])!=utility:abort(400,description='Water and electricity meters cannot be linked together.')
    former_parents={meters[c]['parent_meter_id'] for c in children if meters[c]['parent_meter_id'] not in (None,parent)}
    graph={r['id']:r['parent_meter_id'] for r in rows}
    for child in graph:
        if graph[child]==parent:graph[child]=None
    for child in children:graph[child]=parent
    for child in graph:
        visited=set();node=child
        while node in graph and node is not None:
            if node in visited:abort(400,description='These links would create a circular relationship.')
            visited.add(node);node=graph[node]
    db.session.execute(text('UPDATE bil_meter SET parent_meter_id=NULL WHERE property_id=:p AND parent_meter_id=:parent'),dict(p=p,parent=parent))
    for child in children:db.session.execute(text('UPDATE bil_meter SET parent_meter_id=:parent WHERE id=:child AND property_id=:p'),dict(p=p,parent=parent,child=child))
    for former in sorted(former_parents):
        old_children=[r['id'] for r in rows if r['parent_meter_id']==former]
        audit(p,'bulk_link',former,dict(children=old_children),dict(children=[c for c in old_children if c not in children],source='bulk_meter_linking',reassigned_to=parent))
    audit(p,'bulk_link',parent,dict(children=[r['id'] for r in rows if r['parent_meter_id']==parent]),dict(children=children,source='bulk_meter_linking'))

def confirmed_meters(p,rows,session=None):
    """Legacy pointers alone never establish an explicitly authorised bulk link."""
    from .utilities_capture import available
    confirmations={}
    if available('bil_utilities_capture_audit',['property_id','kind','record_id','after_data']):
        for r in (session if session is not None else db.session).execute(text("SELECT DISTINCT ON (record_id) record_id,after_data FROM bil_utilities_capture_audit WHERE property_id=:p AND kind='bulk_link' ORDER BY record_id,id DESC"),dict(p=p)).mappings():
            data=r['after_data']
            if data.get('source')=='bulk_meter_linking':confirmations[r['record_id']]=set(data.get('children',[]))
    result=[]
    for row in rows:
        meter=dict(row);parent=meter.get('parent_meter_id')
        if meter['id'] not in confirmations.get(parent,set()):meter['parent_meter_id']=None
        result.append(meter)
    return result

def page(p):
    property_scope(p)
    from .municipal_metsoa import month_context
    month,_,_=month_context(request.values.get('billing_month'))
    error=None;status=200
    if request.method=='POST':
        try:
            save(p,request.form);db.session.commit();flash('Bulk meter links saved.','success')
            return redirect(url_for('billing_bp.bulk_meter_linking',property_id=p,billing_month=month,edit=request.form.get('parent_id')))
        except HTTPException as exc:db.session.rollback();error=exc.description;status=exc.code
        except Exception:
            db.session.rollback();current_app.logger.exception('Bulk meter linking failed');error='Links could not be saved. No changes were committed.';status=503
    prop=db.session.execute(text('SELECT id,name FROM bil_property WHERE id=:p'),dict(p=p)).mappings().one()
    rows=[dict(r) for r in db.session.execute(text('SELECT id,meter_number,utility_type,parent_meter_id FROM bil_meter WHERE property_id=:p ORDER BY meter_number,id'),dict(p=p)).mappings()]
    selected=request.form.get('parent_id') if request.method=='POST' else request.args.get('edit','')
    return render_template('program_billing/bulk_meter_linking.html',property=prop,utility_month=month,meters=rows,selected=selected,kind=kind,error=error),status
