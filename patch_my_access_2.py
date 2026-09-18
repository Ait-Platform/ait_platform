import re
with open("templates/program_uip/my_access.html", "r", encoding="utf-8") as f:
    text = f.read()

old_header = """<header class="ui-page-head" style="margin-bottom: 2rem;">
    <div>
        <p class="ui-eyebrow">Access Request Status</p>
        <h1>{{ current_user.name or "Waiting Guest" }}</h1>
        <p class="ui-subtitle">{{ current_user.email }}</p>
    </div>
</header>"""

new_header = """<header class="ui-page-head flex justify-between items-start" style="margin-bottom: 2rem;">
    <div>
        <p class="ui-eyebrow">Access Request Status</p>
        <h1>{{ current_user.name or "Waiting Guest" }}</h1>
        <p class="ui-subtitle">{{ current_user.email }}</p>
    </div>
    <div class="mt-2">
        <a href="{{ url_for('uip_bp.dashboard', org_slug=org.slug) }}" class="inline-block px-4 py-2 border border-slate-300 rounded-lg text-sm font-bold text-slate-600 hover:bg-slate-50 transition">
            &larr; Back to Dashboard
        </a>
    </div>
</header>"""

text = text.replace(old_header, new_header)

with open("templates/program_uip/my_access.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Replaced header")
