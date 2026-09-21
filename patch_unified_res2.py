with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

# Header update
old_header = """<div class="mb-8 flex justify-between items-start">
        <div class="flex-grow pr-8">
            <div class="text-lg font-black text-slate-800 uppercase tracking-widest mb-4 border-b border-slate-200 pb-2 flex items-center">
                <i class="fas fa-landmark text-indigo-600 mr-3 text-xl"></i> 
                {% if resolution.status == 'ADOPTED' or resolution.status == 'REJECTED' %}Historical Resolution Record{% elif resolution.status == 'DRAFT' %}Drafting Desk{% else %}Digital Voting Room{% endif %}
            </div>
            <a href="{{ url_for('uip_bp.dashboard', org_slug=org.slug) }}" class="text-slate-500 hover:text-indigo-600 font-bold text-sm inline-flex items-center transition bg-white border border-slate-200 px-3 py-1.5 rounded-lg shadow-sm mb-6"><i class="fas fa-arrow-left mr-2"></i> Back to Dashboard</a>
            
            <div class="text-xs font-black text-indigo-600 mb-1 tracking-widest uppercase">{{ resolution.reference }}</div>
            <h1 class="text-3xl font-extrabold text-slate-900 flex items-center mb-2">
                <i class="fas fa-file-signature text-indigo-700 mr-3"></i> {{ resolution.title }}
            </h1>
        </div>"""

new_header = """<div class="mb-8 flex justify-between items-start">
        <div class="flex-grow pr-8">
            <!-- Row 1: Title and Back Button inline -->
            <div class="text-lg font-black text-slate-800 uppercase tracking-widest mb-4 border-b border-slate-200 pb-2 flex items-center justify-between">
                <div>
                    <i class="fas fa-landmark text-indigo-600 mr-3 text-xl"></i> 
                    {% if resolution.status == 'ADOPTED' or resolution.status == 'REJECTED' %}Historical Resolution Record{% elif resolution.status == 'DRAFT' %}Drafting Desk{% else %}Digital Voting Room{% endif %}
                </div>
                <a href="{{ back_url|default(url_for('uip_bp.committee_dashboard', org_slug=org.slug)) }}" class="text-slate-500 hover:text-indigo-600 font-bold text-sm inline-flex items-center transition bg-white border border-slate-200 px-3 py-1.5 rounded-lg shadow-sm">
                    <i class="fas fa-arrow-left mr-2"></i> {{ back_text|default('Back to Voting & Mandates') }}
                </a>
            </div>
            
            <div class="text-xs font-black text-indigo-600 mb-1 tracking-widest uppercase">{{ resolution.reference }}</div>
            <h1 class="text-3xl font-extrabold text-slate-900 flex items-center mb-2">
                <i class="fas fa-file-signature text-indigo-700 mr-3"></i> {{ resolution.title }}
            </h1>
        </div>"""

text = text.replace(old_header, new_header)

# Colors update
old_yea = '<button type="submit" name="vote" value="YEA" class="flex-1 py-3 rounded-lg font-black text-white bg-emerald-600 hover:bg-emerald-700 transition shadow-sm text-center">'
new_yea = '<button type="submit" name="vote" value="YEA" class="flex-1 py-3 rounded-lg font-black text-emerald-700 bg-emerald-50 border border-emerald-200 hover:bg-emerald-100 hover:border-emerald-300 transition shadow-sm text-center">'
text = text.replace(old_yea, new_yea)

old_nay = '<button type="submit" name="vote" value="NAY" class="flex-1 py-3 rounded-lg font-black text-white bg-red-600 hover:bg-red-700 transition shadow-sm text-center">'
new_nay = '<button type="submit" name="vote" value="NAY" class="flex-1 py-3 rounded-lg font-black text-rose-700 bg-rose-50 border border-rose-200 hover:bg-rose-100 hover:border-rose-300 transition shadow-sm text-center">'
text = text.replace(old_nay, new_nay)

old_abstain = '<button type="submit" name="vote" value="ABSTAIN" class="flex-1 py-3 rounded-lg font-black text-slate-600 bg-white border border-slate-300 hover:bg-slate-50 transition shadow-sm text-center">'
new_abstain = '<button type="submit" name="vote" value="ABSTAIN" class="flex-1 py-3 rounded-lg font-black text-amber-700 bg-amber-50 border border-amber-200 hover:bg-amber-100 hover:border-amber-300 transition shadow-sm text-center">'
text = text.replace(old_abstain, new_abstain)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Applied headers and colors")
