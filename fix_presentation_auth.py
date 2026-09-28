import re

filepath = 'app/program_uip/presentation.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

new_issue_rows = '''def issue_rows(org, actor):
    from werkzeug.exceptions import Forbidden
    from app.program_uip.secretary_routes import _require_secretary
    try:
        audit.authorize(org, actor, providers.STAFF)
    except Forbidden:
        _require_secretary()
        
    members, properties, _ = register_links(org)'''

content = re.sub(r'def issue_rows\(org, actor\):\s+audit\.authorize\(org, actor, providers\.STAFF\)\s+members, properties, _ = register_links\(org\)', new_issue_rows, content)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated issue_rows to allow UIP committee secretary")
