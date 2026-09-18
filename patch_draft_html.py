with open("templates/program_uip/dashboards/draft_resolution.html", "r", encoding="utf-8") as f:
    text = f.read()

old_header = """<header class="ui-page-head mb-8 border-b border-slate-200 pb-6">
    <a href="{{ url_for('uip_bp.committee_dashboard', org_slug=org.slug) }}" class="text-sm font-bold text-slate-500 hover:text-slate-800 transition mb-4 inline-block">&larr; Back to Dashboard</a>
    <h1 class="text-3xl font-extrabold text-slate-900">Draft New Resolution</h1>
    <p class="text-slate-600 mt-2">Write the formal text of the resolution. It will be saved as a Draft and won't be visible to the committee until you publish it.</p>
</header>"""

new_header = """<header class="ui-page-head mb-8 border-b border-slate-200 pb-6">
    {% if current_appointment and current_appointment.position == "Secretary" %}
    <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}" class="inline-flex items-center text-sm font-bold text-emerald-600 hover:text-emerald-800 mb-3 bg-emerald-50 px-3 py-1 rounded-full transition">
        <i class="fas fa-arrow-left mr-2"></i> Back to Command Switchboard
    </a>
    {% else %}
    <a href="{{ url_for('uip_bp.committee_dashboard', org_slug=org.slug) }}" class="text-sm font-bold text-slate-500 hover:text-slate-800 transition mb-4 inline-block">&larr; Back to Dashboard</a>
    {% endif %}
    <h1 class="text-3xl font-extrabold text-slate-900">Draft New Resolution</h1>
    <p class="text-slate-600 mt-2">Write the formal text of the resolution. It will be saved as a Draft and won't be visible to the committee until you publish it.</p>
</header>"""

text = text.replace(old_header, new_header)

with open("templates/program_uip/dashboards/draft_resolution.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Injected boomerang into draft_resolution.html")
