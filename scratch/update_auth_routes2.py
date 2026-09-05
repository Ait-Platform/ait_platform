import re

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the old cptd block and the one I just injected
old_cptd_block = """        elif slug == 'cptd':
            return redirect(url_for('cptd_bp.hub'))
        elif 'sace' in slug or 'cptd' in slug:
            return redirect(url_for('sace_bp.participant_onboarding'))"""

new_cptd_block = """        elif 'sace' in slug or 'cptd' in slug:
            return redirect(url_for('sace_bp.participant_onboarding'))"""

content = content.replace(old_cptd_block, new_cptd_block)

with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
