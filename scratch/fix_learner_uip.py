import re

routes_path = 'app/auth/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_subject = '''    # -------------------------------------------------
    # Determine start URL for generic "Press Next" screen
    # -------------------------------------------------
    if slug in ('hiq', 'healthcore'):
        start_url = url_for("healthcore_bp.healthcore_dashboard")
    elif row.get("start_endpoint"):'''

new_subject = '''    # -------------------------------------------------
    # Determine start URL for generic "Press Next" screen
    # -------------------------------------------------
    if slug in ('hiq', 'healthcore'):
        start_url = url_for("healthcore_bp.healthcore_dashboard")
    elif slug == 'uip':
        start_url = url_for("uip_bp.org_dashboard", org_slug="manor-gardens")
    elif row.get("start_endpoint"):'''

text = text.replace(old_subject, new_subject)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
