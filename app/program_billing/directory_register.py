"""Browser-local report destinations; no filesystem paths or files on the server."""
from flask import request, current_app
from flask_login import current_user, login_required
from sqlalchemy import text
from app.extensions import db
from .routes import billing_bp
from .lease_management import require_pm_user, render_pm_template
from .metsoa_setup_safety import property_scope
from .utilities_output_reports import CATALOG, SOURCES


@billing_bp.get('/billing/directory-register', endpoint='directory_register')
@login_required
def directory_register():
    require_pm_user()
    selected = request.args.get('property_id')
    if selected:
        selected = property_scope(selected)
    properties = [dict(row) for row in db.session.execute(text(
        'SELECT id,name FROM bil_property WHERE manager_id=:u ORDER BY name,id'),
        dict(u=current_user.id)).mappings()]
    selected = next((p for p in properties if p['id'] == selected), properties[0] if properties else None)
    response = current_app.make_response(render_pm_template('program_billing/directory_register.html',
        properties=properties, selected_property=selected,
        directory_context=dict(user=str(current_user.id), property=str(selected['id']) if selected else ''),
        directory_reports=[dict(code=code,title=title,source=SOURCES[source]) for source,reports in CATALOG.items() for code,title in reports]))
    response.headers['Cache-Control'] = 'private, no-store'
    return response
