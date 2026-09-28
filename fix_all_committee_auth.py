import re

# Update presentation.py
filepath_presentation = 'app/program_uip/presentation.py'
with open(filepath_presentation, 'r', encoding='utf-8') as f:
    content = f.read()

new_issue_rows = '''def issue_rows(org, actor):
    from werkzeug.exceptions import Forbidden
    try:
        audit.authorize(org, actor, providers.STAFF)
    except Forbidden:
        from flask_login import current_user
        from app.models.uip_governance import UipCommitteeMember
        from sqlalchemy import func
        is_committee = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org,
            UipCommitteeMember.status == "CURRENT",
            func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
        ).first()
        if not is_committee:
            raise
            
    members, properties, _ = register_links(org)'''

content = re.sub(r'def issue_rows\(org, actor\):\s+from werkzeug\.exceptions import Forbidden\s+from app\.program_uip\.secretary_routes import _require_secretary\s+try:\s+audit\.authorize\(org, actor, providers\.STAFF\)\s+except Forbidden:\s+_require_secretary\(\)\s+members, properties, _ = register_links\(org\)', new_issue_rows, content)

with open(filepath_presentation, 'w', encoding='utf-8') as f:
    f.write(content)


# Update operational_routes.py
filepath_routes = 'app/program_uip/operational_routes.py'
with open(filepath_routes, 'r', encoding='utf-8') as f:
    routes_content = f.read()

auth_block = '''    from werkzeug.exceptions import Forbidden
    try:
        audit.authorize(org, actor, providers.STAFF)
    except Forbidden:
        from app.models.uip_governance import UipCommitteeMember
        from sqlalchemy import func
        is_committee = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org,
            UipCommitteeMember.status == "CURRENT",
            func.lower(UipCommitteeMember.email) == func.lower(current_user.email)
        ).first()
        if not is_committee:
            raise Forbidden("Access restricted to Staff and Committee members.")'''

old_auth_block = '''    from werkzeug.exceptions import Forbidden
    try:
        audit.authorize(org, actor, providers.STAFF)
    except Forbidden:
        from app.program_uip.secretary_routes import _require_secretary
        _require_secretary()'''

routes_content = routes_content.replace(old_auth_block, auth_block)

with open(filepath_routes, 'w', encoding='utf-8') as f:
    f.write(routes_content)

print("Updated authorization to allow all ExCo and SubCom members")
