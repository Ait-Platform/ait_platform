"""Account-scoped meter exceptions; legacy meter history remains read-only."""
from flask import request,render_template,abort
from sqlalchemy import text
from app.extensions import db
from .metsoa_setup_safety import property_scope

def accounts(p):
    return [dict(r) for r in db.session.execute(text('SELECT a.*,o.name AS owner_name FROM bil_muni_account a LEFT JOIN ref_muni_owner o ON o.id=a.owner_id WHERE a.property_id=:p ORDER BY a.account_number,a.id'),dict(p=p)).mappings()]

def account(p,value):
    try:identifier=int(value)
    except (TypeError,ValueError):abort(400,description='Select a municipal account.')
    row=next((a for a in accounts(p) if a['id']==identifier),None)
    if not row:abort(403)
    return row

def account_meters(p,acc):
    rows=[dict(r) for r in db.session.execute(text('SELECT * FROM bil_meter WHERE property_id=:p AND (id=:w OR id=:e OR municipal_bill_number=:n) ORDER BY meter_number,id'),dict(p=p,w=acc['water_meter_id'],e=acc['elec_meter_id'],n=acc['account_number'])).mappings()]
    return rows

def page(p):
    property_scope(p)
    prop=db.session.execute(text('SELECT id,name FROM bil_property WHERE id=:p'),dict(p=p)).mappings().one()
    acc=account(p,request.args['account_id']) if request.args.get('account_id') else None
    meters=account_meters(p,acc) if acc else [dict(r) for r in db.session.execute(text('SELECT * FROM bil_meter WHERE property_id=:p ORDER BY meter_number,id'),dict(p=p)).mappings()]
    legacy=[m for m in meters if (m.get('status') or 'active').lower()!='active']
    return render_template('program_billing/meter_exceptions.html',property=prop,accounts=accounts(p),account=acc,meters=meters,legacy=legacy,utility_month=request.args.get('billing_month',''))
