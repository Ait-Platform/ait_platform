with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

old_block = """                <h3 class="font-bold text-slate-900 mb-2 uppercase tracking-widest text-xs">Live Tally & Participation</h3>
                
                <div class="mb-2 flex justify-between text-sm font-bold">
                    <span class="text-slate-600">Current: {{ current_quorum_pct }}%</span>
                    <span class="text-indigo-600">Target: {{ quorum_target }}%</span>
                </div>
                
                <div class="w-full bg-slate-100 rounded-full h-3 mb-4 overflow-hidden">
                    <div class="{% if quorum_met %}bg-emerald-500{% else %}bg-amber-500{% endif %} h-3 rounded-full transition-all duration-500" style="width: {{ current_quorum_pct }}%"></div>
                </div>"""

new_block = """                <h3 class="font-bold text-slate-900 mb-2 uppercase tracking-widest text-xs">Live Tally & Participation</h3>
                
                <div class="mb-2 flex justify-between text-sm font-bold">
                    <span class="text-slate-600">Participation Rate</span>
                    <span class="text-indigo-600">{{ current_quorum_pct }}%</span>
                </div>
                
                <div class="w-full bg-slate-100 rounded-full h-3 mb-4 overflow-hidden border border-slate-200">
                    <div class="bg-indigo-500 h-3 rounded-full transition-all duration-500" style="width: {{ current_quorum_pct }}%"></div>
                </div>"""

text = text.replace(old_block, new_block)
with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched view HTML")
