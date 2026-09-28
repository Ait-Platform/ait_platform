import re

filepath = 'app/program_uip/secretary_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

bad_auth = '''    org = CoreOrganization.query.filter_by(slug=org_slug).first_or_404()
    if not _require_role("secretary", abort_on_fail=False) and not _require_role("manager", abort_on_fail=False):
        abort(403)'''

good_auth = '''    org = CoreOrganization.query.filter_by(slug=org_slug).first_or_404()
    _require_secretary()'''

content = content.replace(bad_auth, good_auth)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Auth fixed in secretary_routes.py")
