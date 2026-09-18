import re
with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

old_header = """    <div class="mb-8 flex justify-between items-start">
        <div>
            <div class="text-xs font-black text-indigo-600 mb-1 tracking-widest uppercase">{{ resolution.reference }}</div>
            <h1 class="text-3xl font-extrabold text-slate-900 flex items-center mb-4">
                <i class="fas fa-file-signature text-indigo-700 mr-3"></i> {{ resolution.title }}
            </h1>
            <a href="{{ url_for('uip_bp.dashboard', org_slug=org.slug) }}" class="text-slate-500 hover:text-indigo-600 font-bold text-sm inline-flex items-center transition bg-white border border-slate-200 px-3 py-1.5 rounded-lg shadow-sm"><i class="fas fa-arrow-left mr-2"></i> Back to Dashboard</a>
        </div>"""

new_header = """    <div class="mb-8 flex justify-between items-start">
        <div>
            <div class="text-xs font-bold text-slate-500 uppercase tracking-widest mb-3">
                <i class="fas fa-landmark text-indigo-500 mr-2"></i> 
                {% if resolution.status == 'ADOPTED' %}Historical Resolution Record{% elif resolution.status == 'DRAFT' %}Drafting Desk{% else %}Digital Voting Room{% endif %}
            </div>
            <a href="{{ url_for('uip_bp.dashboard', org_slug=org.slug) }}" class="text-slate-500 hover:text-indigo-600 font-bold text-sm inline-flex items-center transition bg-white border border-slate-200 px-3 py-1.5 rounded-lg shadow-sm mb-6"><i class="fas fa-arrow-left mr-2"></i> Back to Dashboard</a>
            
            <div class="text-xs font-black text-indigo-600 mb-1 tracking-widest uppercase">{{ resolution.reference }}</div>
            <h1 class="text-3xl font-extrabold text-slate-900 flex items-center mb-2">
                <i class="fas fa-file-signature text-indigo-700 mr-3"></i> {{ resolution.title }}
            </h1>
        </div>"""

text = text.replace(old_header, new_header)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated header")
