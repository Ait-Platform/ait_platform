import re

routes_path = 'app/auth/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_uip_logic = '''        elif subject == "uip":
            # UIP has no central router yet, we can default to their org dashboard if known, 
            # or to the welcome page if org is unknown. Let's send them to bridge for now which will resolve it.
            return redirect(url_for("auth_bp.bridge_dashboard"))'''

new_uip_logic = '''        elif subject == "uip":
            return redirect(url_for("uip_bp.org_dashboard", org_slug="manor-gardens"))'''

text = text.replace(old_uip_logic, new_uip_logic)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
