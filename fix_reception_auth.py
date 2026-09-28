import re

filepath = 'app/program_uip/operational_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Fix reception_page
new_reception_page = '''@uip_bp.route("/<org_slug>/operations/reception", methods=["GET"])
@login_required
def reception_page(org_slug):
    org, actor = g.organization.id, current_user.id
    from werkzeug.exceptions import Forbidden
    try:
        audit.authorize(org, actor, providers.STAFF)
    except Forbidden:
        from app.program_uip.secretary_routes import _require_secretary
        _require_secretary()
'''
content = re.sub(r'@uip_bp\.route\("/<org_slug>/operations/reception", methods=\["GET"\]\)\s+@login_required\s+def reception_page\(org_slug\):\s+org, actor = g\.organization\.id, current_user\.id\s+audit\.authorize\(org, actor, providers\.STAFF\)', new_reception_page, content)

# Fix reception_issue
new_reception_issue = '''@uip_bp.route("/<org_slug>/operations/reception/<int:issue_id>", methods=["GET", "POST"])
@login_required
def reception_issue(org_slug, issue_id):
    from app.models.uip import UipWorkOrder
    org, actor = g.organization.id, current_user.id
    from werkzeug.exceptions import Forbidden
    try:
        audit.authorize(org, actor, providers.STAFF)
    except Forbidden:
        from app.program_uip.secretary_routes import _require_secretary
        _require_secretary()
'''
content = re.sub(r'@uip_bp\.route\("/<org_slug>/operations/reception/<int:issue_id>", methods=\["GET", "POST"\]\)\s+@login_required\s+def reception_issue\(org_slug, issue_id\):\s+from app\.models\.uip import UipWorkOrder\s+org, actor = g\.organization\.id, current_user\.id\s+audit\.authorize\(org, actor, providers\.STAFF\)', new_reception_issue, content)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated reception page access to allow UIP committee secretary")
