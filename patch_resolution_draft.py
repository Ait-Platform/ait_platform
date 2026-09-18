with open("templates/program_uip/dashboards/resolution_draft.html", "r", encoding="utf-8") as f:
    text = f.read()

old_link = """<a href="{{ url_for('uip_bp.committee_dashboard', org_slug=org.slug) }}" class="text-indigo-600 font-bold text-sm mb-2 inline-block">&larr; Back to Dashboard</a>"""

new_link = """{% if current_appointment and current_appointment.position == "Secretary" %}
            <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}" class="inline-flex items-center text-sm font-bold text-emerald-600 hover:text-emerald-800 mb-3 bg-emerald-50 px-3 py-1 rounded-full transition">
                <i class="fas fa-arrow-left mr-2"></i> Back to Command Switchboard
            </a>
            {% else %}
            <a href="{{ url_for('uip_bp.committee_dashboard', org_slug=org.slug) }}" class="text-indigo-600 font-bold text-sm mb-2 inline-block">&larr; Back to Dashboard</a>
            {% endif %}"""

text = text.replace(old_link, new_link)

with open("templates/program_uip/dashboards/resolution_draft.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Injected boomerang into resolution_draft.html")
