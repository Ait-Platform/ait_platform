lines = []
with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "View &rarr;</a>" in line and "url_for('uip_bp.view_resolution'" in line:
        print(f"Found at line {i}: {line.strip()}")
        lines[i] = """                          {% if res.status == 'DRAFT' %}
                          <a href="{{ url_for('uip_bp.edit_resolution', org_slug=org.slug, res_id=res.id) }}" class="text-amber-700 hover:text-amber-900 font-bold bg-amber-50 px-3 py-1 rounded inline-block transition shadow-sm text-[11px] uppercase tracking-wider border border-amber-200"><i class="fas fa-pen-nib mr-1"></i> Edit Draft</a>
                          {% elif res.status == 'PROPOSED' %}
                          <a href="{{ url_for('uip_bp.view_resolution', org_slug=org.slug, res_id=res.id) }}" class="text-indigo-700 hover:text-indigo-900 font-bold bg-indigo-50 px-3 py-1 rounded inline-block transition shadow-sm text-[11px] uppercase tracking-wider border border-indigo-200">Vote &rarr;</a>
                          {% else %}
                          <a href="{{ url_for('uip_bp.view_resolution', org_slug=org.slug, res_id=res.id) }}" class="text-slate-600 hover:text-slate-900 font-bold bg-slate-100 px-3 py-1 rounded inline-block transition shadow-sm text-[11px] uppercase tracking-wider border border-slate-200">View &rarr;</a>
                          {% endif %}
"""
    if "bg-slate-100 text-slate-800\">{{ res.status }}</span>" in line:
        print(f"Found status block at line {i}")
        lines[i] = """                          {% elif res.status == 'DRAFT' %}
                              <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-amber-100 text-amber-800">Draft</span>
                          {% else %}
                              <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-slate-100 text-slate-800">{{ res.status }}</span>\n"""

with open("templates/program_uip/dashboards/committee.html", "w", encoding="utf-8") as f:
    f.writelines(lines)
