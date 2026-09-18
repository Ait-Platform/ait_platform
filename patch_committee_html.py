with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    text = f.read()

old_header = """<header class="ui-page-head flex justify-between items-start mb-8 border-b border-slate-200 pb-6">
    <div>
        <h1 class="text-3xl font-extrabold text-slate-900 mt-1">{% if current_appointment and current_appointment.position == "Secretary" %}Secretary{% else %}Committee{% endif %} Dashboard</h1>"""

new_header = """<header class="ui-page-head flex justify-between items-start mb-8 border-b border-slate-200 pb-6">
    <div>
        {% if current_appointment and current_appointment.position == "Secretary" %}
        <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}" class="inline-flex items-center text-sm font-bold text-indigo-600 hover:text-indigo-800 mb-3 bg-indigo-50 px-3 py-1 rounded-full transition">
            <i class="fas fa-arrow-left mr-2"></i> Back to Command Switchboard
        </a>
        {% endif %}
        <h1 class="text-3xl font-extrabold text-slate-900 mt-1">{% if current_appointment and current_appointment.position == "Secretary" %}Resolution Register{% else %}Committee Dashboard{% endif %}</h1>"""

text = text.replace(old_header, new_header)

with open("templates/program_uip/dashboards/committee.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Injected boomerang into committee.html")
