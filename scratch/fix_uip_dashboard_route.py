import re

routes_path = 'app/auth/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('uip_bp.org_dashboard', 'uip_bp.dashboard')

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
