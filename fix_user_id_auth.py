import re

filepath_routes = 'app/program_uip/operational_routes.py'
with open(filepath_routes, 'r', encoding='utf-8') as f:
    routes_content = f.read()

safe_auth_block = '''    from werkzeug.exceptions import Forbidden
    try:
        audit.authorize(org, actor, providers.STAFF)
    except Forbidden:
        from app.models.uip_governance import UipCommitteeMember
        from sqlalchemy import func, or_
        email_check = False
        if current_user.email and current_user.email.strip():
            email_check = func.lower(func.trim(UipCommitteeMember.email)) == current_user.email.strip().lower()
        is_committee = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org,
            UipCommitteeMember.status == "CURRENT",
            or_(UipCommitteeMember.user_id == actor, email_check)
        ).first()
        if not is_committee:
            raise Forbidden("Access restricted to Staff and Committee members.")'''

# Replace in operational_routes.py
old_auth_block_pattern = r'    from werkzeug\.exceptions import Forbidden\n    try:\n        audit\.authorize\(org, actor, providers\.STAFF\)\n    except Forbidden:\n        from app\.models\.uip_governance import UipCommitteeMember\n        from sqlalchemy import func\n        is_committee = UipCommitteeMember\.query\.filter\(\n            UipCommitteeMember\.organization_id == org,\n            UipCommitteeMember\.status == "CURRENT",\n            func\.lower\(UipCommitteeMember\.email\) == func\.lower\(current_user\.email\)\n        \)\.first\(\)\n        if not is_committee:\n            raise Forbidden\("Access restricted to Staff and Committee members\."\)'
routes_content = re.sub(old_auth_block_pattern, safe_auth_block, routes_content)

with open(filepath_routes, 'w', encoding='utf-8') as f:
    f.write(routes_content)


filepath_pres = 'app/program_uip/presentation.py'
with open(filepath_pres, 'r', encoding='utf-8') as f:
    pres_content = f.read()

safe_pres_block = '''def issue_rows(org, actor):
    from werkzeug.exceptions import Forbidden
    try:
        audit.authorize(org, actor, providers.STAFF)
    except Forbidden:
        from flask_login import current_user
        from app.models.uip_governance import UipCommitteeMember
        from sqlalchemy import func, or_
        email_check = False
        if current_user.email and current_user.email.strip():
            email_check = func.lower(func.trim(UipCommitteeMember.email)) == current_user.email.strip().lower()
        is_committee = UipCommitteeMember.query.filter(
            UipCommitteeMember.organization_id == org,
            UipCommitteeMember.status == "CURRENT",
            or_(UipCommitteeMember.user_id == actor, email_check)
        ).first()
        if not is_committee:
            raise'''
            
old_pres_pattern = r'def issue_rows\(org, actor\):\n    from werkzeug\.exceptions import Forbidden\n    try:\n        audit\.authorize\(org, actor, providers\.STAFF\)\n    except Forbidden:\n        from flask_login import current_user\n        from app\.models\.uip_governance import UipCommitteeMember\n        from sqlalchemy import func\n        is_committee = UipCommitteeMember\.query\.filter\(\n            UipCommitteeMember\.organization_id == org,\n            UipCommitteeMember\.status == "CURRENT",\n            func\.lower\(UipCommitteeMember\.email\) == func\.lower\(current_user\.email\)\n        \)\.first\(\)\n        if not is_committee:\n            raise'
pres_content = re.sub(old_pres_pattern, safe_pres_block, pres_content)

with open(filepath_pres, 'w', encoding='utf-8') as f:
    f.write(pres_content)

print("Updated authorization blocks to use safe or_ user_id check")
