import re

filepath = 'app/program_uip/operational_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('def merge_tickets(org_slug):\n    org = g.organization', 'def merge_tickets(org_slug):\n    org = g.organization\n    from app.program_uip.routes import _require_role')
content = content.replace('def escalate_ticket(org_slug, ticket_id):\n    org = g.organization', 'def escalate_ticket(org_slug, ticket_id):\n    org = g.organization\n    from app.program_uip.routes import _require_role')

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("Injected _require_role import")
