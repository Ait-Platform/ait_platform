import re
with open("templates/program_uip/my_access.html", "r", encoding="utf-8") as f:
    text = f.read()

old_header = """<header class="ui-page-head mb-12">
    <div>
        <p class="ui-eyebrow">Access Request Status</p>
        <h1>{{ current_user.name or "Waiting Guest" }}</h1>
        <p class="ui-subtitle">{{ current_user.email }}</p>
    </div>
</header>"""

new_header = """<header class="ui-page-head mb-12 flex justify-between items-start">
    <div>
        <p class="ui-eyebrow">Access Request Status</p>
        <h1>{{ current_user.name or "Waiting Guest" }}</h1>
        <p class="ui-subtitle">{{ current_user.email }}</p>
    </div>
    <div class="mt-2">
        <a href="{{ url_for('uip_bp.dashboard', org_slug=org.slug) }}" class="ui-btn ui-btn-outline font-bold">
            &larr; Back to Dashboard
        </a>
    </div>
</header>"""

if old_header in text:
    text = text.replace(old_header, new_header)
    print("Added back button to my_access.html")
else:
    print("Failed to add back button. Header not found.")

with open("templates/program_uip/my_access.html", "w", encoding="utf-8") as f:
    f.write(text)
