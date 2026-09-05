import re

routes_path = 'app/auth/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_bridge = '''        elif slug == 'healthcore':
            return redirect(url_for('healthcore_bp.healthcore_dashboard'))
        else:
            return redirect(url_for('auth_bp.dashboard_info', subject=slug))'''

new_bridge = '''        elif slug == 'healthcore':
            return redirect(url_for('healthcore_bp.healthcore_dashboard'))
        elif slug == 'uip':
            # For now, default to Manor Gardens org dashboard
            return redirect(url_for('uip_bp.org_dashboard', org_slug='manor-gardens'))
        else:
            return redirect(url_for('auth_bp.dashboard_info', subject=slug))'''

text = text.replace(old_bridge, new_bridge)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
