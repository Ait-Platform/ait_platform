import re
with open("templates/program_uip/dashboards/secretary_intake.html", "r", encoding="utf-8") as f:
    text = f.read()

# Update back button
old_back = """        <a href="{{ url_for('uip_bp.committee_dashboard', org_slug=org.slug) }}" class="ui-btn ui-btn-outline font-bold">
            &larr; Back to Committee Board
        </a>"""
new_back = """        <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}" class="ui-btn ui-btn-outline font-bold">
            &larr; Back to Switchboard
        </a>"""
text = text.replace(old_back, new_back)

# Remove Resolutions Tracker section
tracker_pattern = re.compile(r'<div class="mb-8">\s*<div class="ui-card">\s*<header class="ui-card-head">\s*<h2>Resolutions Tracker</h2>.*?</div>\s*</div>\s*</div>', re.DOTALL)
text = tracker_pattern.sub("", text)

with open("templates/program_uip/dashboards/secretary_intake.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated secretary_intake.html")
