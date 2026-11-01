"""Read-only, property-scoped Utilities reports."""
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import request,render_template,abort
from sqlalchemy import text
from app.extensions import db
from .metsoa_setup_safety import property_scope

def report(p,args):
    property_scope(p)
    from .utilities_capture import meters,readings
    source=args.get('source','readings')
    if source!='readings':abort(400,description='Select a supported report source.')
    choices=meters(p);item=args.get('item_id','')
    selected=next((m for m in choices if str(m['id'])==item),None) if item else None
    if item and not selected:abort(403)
    rows=[r for r in readings(p) if not item or str(r['meter_id'])==item]
    prop=db.session.execute(text('SELECT id,name,address FROM bil_property WHERE id=:p'),dict(p=p)).mappings().one()
    return dict(property=prop,source=source,title='Meter Readings',choices=choices,item_id=item,item_label=selected['meter_number'] if selected else 'All Meters',rows=rows,generated_at=datetime.now(ZoneInfo('Africa/Johannesburg')),billing_month=args.get('billing_month',''))

def page(p):
    return render_template('program_billing/utilities_output.html',**report(p,request.args))
