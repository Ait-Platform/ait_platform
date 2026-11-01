"""Read-only, property-scoped Utilities reports."""
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import request,render_template,abort
from sqlalchemy import text
from app.extensions import db
from .metsoa_setup_safety import property_scope

def report(p,args):
    property_scope(p)
    from .utilities_capture import meters,readings,statements
    source=args.get('source','readings')
    if source not in ('readings','reconciliation'):abort(400,description='Select a supported report source.')
    choices=meters(p) if source=='readings' else [dict(r) for r in db.session.execute(text('SELECT id,account_number FROM bil_muni_account WHERE property_id=:p ORDER BY account_number,id'),dict(p=p)).mappings()]
    item=args.get('item_id','')
    selected=next((m for m in choices if str(m['id'])==item),None) if item else None
    if item and not selected:abort(403)
    rows=[r for r in (readings(p) if source=='readings' else statements(p)) if not item or str(r['meter_id'] if source=='readings' else r['account_id'])==item]
    prop=db.session.execute(text('SELECT id,name,address FROM bil_property WHERE id=:p'),dict(p=p)).mappings().one()
    return dict(property=prop,source=source,title='Meter Readings' if source=='readings' else 'Metro Reconciliation',choices=choices,item_id=item,item_label=selected['meter_number' if source=='readings' else 'account_number'] if selected else ('All Meters' if source=='readings' else 'All Accounts'),rows=rows,generated_at=datetime.now(ZoneInfo('Africa/Johannesburg')),billing_month=args.get('billing_month',''))

def page(p):
    return render_template('program_billing/utilities_output.html',**report(p,request.args))
