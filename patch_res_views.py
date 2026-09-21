with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

import re

# 1. Remove old left-side back button
text = re.sub(r'<a href="{{ url_for\(\'uip_bp\.dashboard\', org_slug=org\.slug\) }}".*?</a>\s*', '', text)

# 2. Add Top-Right back button for Chairman (we'll just put it at the very top of the flex container)
# Find `<div class="mb-8 flex justify-between items-start">`
# and prepend a clear top row before it.
top_nav = """<!-- Top Navigation -->
<div class="mb-6 flex justify-between items-center w-full">
    <div></div>
    <a href="{{ url_for('uip_bp.chairman_voting_room', org_slug=org.slug) }}" class="inline-flex items-center px-4 py-2 bg-white border border-slate-300 text-sm font-bold text-slate-600 hover:text-indigo-600 hover:border-indigo-300 rounded-lg shadow-sm transition">
        Back to Voting Room <i class="fas fa-arrow-right ml-2"></i>
    </a>
</div>
"""
text = text.replace('<div class="mb-8 flex justify-between items-start">', top_nav + '<div class="mb-8 flex justify-between items-start">')

# 3. Fix voting button check.
# In resolution_view.html: {% if current_appointment and current_appointment.group_level == 'EXCO_CORE' %}
text = text.replace("{% if current_appointment and current_appointment.group_level == 'EXCO_CORE' %}", "{% if True %}")

# 4. Fix voting submission URL.
text = text.replace("url_for('uip_bp.vote_resolution'", "url_for('uip_bp.chairman_vote_resolution'")

# Write for Chairman
with open("templates/program_uip/dashboards/chairman_resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)

# Also fix Treasurer while we are at it
treasurer_text = text.replace("uip_bp.chairman_voting_room", "uip_bp.treasurer_voting_room")
treasurer_text = treasurer_text.replace("uip_bp.chairman_vote_resolution", "uip_bp.treasurer_vote_resolution")

with open("templates/program_uip/dashboards/treasurer_resolution_view.html", "w", encoding="utf-8") as f:
    f.write(treasurer_text)

print("Re-cloned and fixed resolution views")
