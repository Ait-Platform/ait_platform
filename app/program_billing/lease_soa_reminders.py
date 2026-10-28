"""Read-only due-rent reminders; Later is session-only and never posts rent."""
import hashlib
import time
from flask import session
from flask_login import current_user
from sqlalchemy import text
from . import lease_soa_posting as posting
from . import lease_soa_rent_plan as rent
from .lease_soa import tenants_for


def reminder_id(property_id,tenant_id,due):
    return hashlib.sha256(f'{current_user.id}:{property_id}:{tenant_id}:{due}'.encode()).hexdigest()[:24]


def visit():
    saved=session.get('pm_rent_visit') or {}
    if saved.get('until',0)<time.time():saved={'ids':[]}
    saved['until']=time.time()+1800
    session['pm_rent_visit']=saved
    return saved


def reminders():
    from .lease_soa_fixed import require_schema
    require_schema()
    dismissed=visit()['ids'];result=[]
    properties=posting.db.session.execute(text('SELECT id,name FROM bil_property WHERE manager_id=:m AND NOT is_archived ORDER BY id'),dict(m=current_user.id)).mappings().all()
    for prop in properties:
        for tenant in tenants_for(prop['id']):
            if not tenant.get('draft_id'):continue
            try:plan=rent.plan(prop['id'],tenant['id'])
            except ValueError:continue
            # One oldest due reminder per account, not one per page rendering or matching word.
            if reminder_id(prop['id'],tenant['id'],'visit') in dismissed:continue
            row=next((r for r in plan['rows'] if r['can_post']),None)
            if row:
                result.append(dict(property_id=prop['id'],tenant_id=tenant['id'],property_name=prop['name'],tenant_name=tenant['name'],due=row['due'],month=row['month'],key=plan['key']))
    return result


def later(property_id,tenant_id,form):
    from flask import abort
    plan=rent.plan(property_id,tenant_id)
    rent.verify_key(plan,property_id,tenant_id,form)
    row=next((r for r in plan['rows'] if str(r['due'])==form.get('due') and r['can_post']),None)
    if row is None:abort(400,description='This rent month is no longer due for posting.')
    saved=visit();identifier=reminder_id(property_id,tenant_id,'visit')
    if identifier not in saved['ids']:saved['ids']=(saved['ids']+[identifier])[-100:]
    session['pm_rent_visit']=saved
