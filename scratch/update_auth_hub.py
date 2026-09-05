import re

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_routing = """        elif slug == 'sace_evaluator':
            return redirect(url_for('sace_bp.reading_hub'))
        elif slug == 'sace_facilitator':
            return redirect(url_for('sace_bp.facilitator_dashboard'))
        elif slug == 'sace_participant':
            return redirect(url_for('sace_bp.participant_onboarding'))
        elif slug == 'cptd':
            return redirect(url_for('cptd_bp.hub'))"""

new_routing = """        elif slug == 'cptd' or 'sace' in slug:
            return redirect(url_for('sace_bp.selection_hub'))"""

content = content.replace(old_routing, new_routing)

with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated auth routes for selection hub")
