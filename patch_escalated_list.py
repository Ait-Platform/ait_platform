import re
with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    text = f.read()

old_badge = """                          {% elif res.status == 'REJECTED' %}
                              <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-red-100 text-red-800">Rejected</span>
                          {% elif res.status == 'DRAFT' %}"""

new_badge = """                          {% elif res.status == 'REJECTED' %}
                              <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-red-100 text-red-800">Rejected</span>
                          {% elif res.status == 'ESCALATED' %}
                              <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-red-100 text-red-800">Escalated</span>
                          {% elif res.status == 'DRAFT' %}"""
text = text.replace(old_badge, new_badge)

old_btn = """                          {% elif res.status == 'PROPOSED' %}
                          <a href="{{ url_for('uip_bp.view_resolution', org_slug=org.slug, res_id=res.id) }}" class="text-indigo-700 hover:text-indigo-900 font-bold bg-indigo-50 px-3 py-1 rounded inline-block transition shadow-sm text-[11px] uppercase tracking-wider border border-indigo-200">Vote &rarr;</a>
                          {% else %}"""

new_btn = """                          {% elif res.status == 'PROPOSED' %}
                          <a href="{{ url_for('uip_bp.view_resolution', org_slug=org.slug, res_id=res.id) }}" class="text-indigo-700 hover:text-indigo-900 font-bold bg-indigo-50 px-3 py-1 rounded inline-block transition shadow-sm text-[11px] uppercase tracking-wider border border-indigo-200">Vote &rarr;</a>
                          {% elif res.status == 'ESCALATED' %}
                          <a href="{{ url_for('uip_bp.view_resolution', org_slug=org.slug, res_id=res.id) }}" class="text-red-700 hover:text-red-900 font-bold bg-red-50 px-3 py-1 rounded inline-block transition shadow-sm text-[11px] uppercase tracking-wider border border-red-200">Ratify &rarr;</a>
                          {% else %}"""
text = text.replace(old_btn, new_btn)

with open("templates/program_uip/dashboards/committee.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated committee.html for ESCALATED")
