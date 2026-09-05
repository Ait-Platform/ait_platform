import re

# 1. Update routes.py
with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    routes = f.read()

routes = routes.replace(
    "@sace_bp.route('/sace/participant/onboarding', methods=['GET', 'POST'])\n@login_required\ndef participant_onboarding():",
    "@sace_bp.route('/sace/participant/<activity_slug>/onboarding', methods=['GET', 'POST'])\n@login_required\ndef participant_onboarding(activity_slug):"
)
routes = routes.replace(
    "return render_template('program_sace/onboarding.html')",
    "return render_template('program_sace/onboarding.html', activity_slug=activity_slug)"
)
with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(routes)

# 2. Update sace_selection_hub.html
with open('templates/program_sace/sace_selection_hub.html', 'r', encoding='utf-8') as f:
    hub = f.read()

hub = hub.replace(
    "url_for('sace_bp.participant_onboarding')",
    "url_for('sace_bp.participant_onboarding', activity_slug=activity_slug)"
)
with open('templates/program_sace/sace_selection_hub.html', 'w', encoding='utf-8') as f:
    f.write(hub)

# 3. Update onboarding.html
with open('templates/program_sace/onboarding.html', 'r', encoding='utf-8') as f:
    onb = f.read()

onb = onb.replace(
    "url_for('sace_bp.selection_hub')",
    "url_for('sace_bp.selection_hub', activity_slug=activity_slug)"
)
onb = onb.replace(
    "url_for('sace_bp.participant_onboarding')",
    "url_for('sace_bp.participant_onboarding', activity_slug=activity_slug)"
)
with open('templates/program_sace/onboarding.html', 'w', encoding='utf-8') as f:
    f.write(onb)

print("Updated participant_onboarding to accept activity_slug")
