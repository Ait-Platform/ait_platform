import re

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Add routing for sace/cptd slugs in bridge_dashboard
injection = """        elif 'sace' in slug or 'cptd' in slug:
            return redirect(url_for('sace_bp.participant_onboarding'))
        elif slug == 'mechanic':"""
content = content.replace("        elif slug == 'mechanic':", injection)

with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
