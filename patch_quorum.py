import re
with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

old_quorum = """            <!-- Quorum Tracker -->
            <div class="bg-white border border-slate-200 rounded-xl shadow-sm p-6">
                <h3 class="font-bold text-slate-900 mb-2 uppercase tracking-widest text-xs">Live Tally & Quorum</h3>
                
                <div class="mb-2 flex justify-between text-sm font-bold">
                    <span class="text-slate-600">Current: {{ current_quorum_pct }}%</span>
                    <span class="text-indigo-600">Target: {{ quorum_target }}%</span>
                </div>
                
                <div class="w-full bg-slate-100 rounded-full h-3 mb-4 overflow-hidden">
                    <div class="{% if quorum_met %}bg-emerald-500{% else %}bg-amber-500{% endif %} h-3 rounded-full transition-all duration-500" style="width: {{ current_quorum_pct }}%"></div>
                </div>
                
                <div class="flex justify-between border-t border-slate-100 pt-3 mt-3 text-sm font-bold">
                    <div class="text-emerald-600">YEA: {{ yea_count }}</div>
                    <div class="text-red-600">NAY: {{ nay_count }}</div>
                    <div class="text-slate-500">ABSTAIN: {{ abstain_count }}</div>
                </div>
            </div>"""

new_quorum = """            <!-- Status & Quorum -->
            {% if resolution.status == 'PROPOSED' %}
            <div class="bg-white border border-slate-200 rounded-xl shadow-sm p-6">
                <h3 class="font-bold text-slate-900 mb-2 uppercase tracking-widest text-xs">Live Tally & Quorum</h3>
                
                <div class="mb-2 flex justify-between text-sm font-bold">
                    <span class="text-slate-600">Current: {{ current_quorum_pct }}%</span>
                    <span class="text-indigo-600">Target: {{ quorum_target }}%</span>
                </div>
                
                <div class="w-full bg-slate-100 rounded-full h-3 mb-4 overflow-hidden">
                    <div class="{% if quorum_met %}bg-emerald-500{% else %}bg-amber-500{% endif %} h-3 rounded-full transition-all duration-500" style="width: {{ current_quorum_pct }}%"></div>
                </div>
                
                <div class="flex justify-between border-t border-slate-100 pt-3 mt-3 text-sm font-bold">
                    <div class="text-emerald-600">YEA: {{ yea_count }}</div>
                    <div class="text-red-600">NAY: {{ nay_count }}</div>
                    <div class="text-slate-500">ABSTAIN: {{ abstain_count }}</div>
                </div>
            </div>
            {% else %}
            <div class="bg-slate-50 border border-slate-200 rounded-xl shadow-sm p-6 text-center">
                <i class="fas fa-landmark text-4xl text-slate-300 mb-4 block"></i>
                <h3 class="font-bold text-slate-700 mb-1">Historical Record</h3>
                <p class="text-sm text-slate-500">This resolution is officially adopted and closed for voting.</p>
            </div>
            {% endif %}"""

text = text.replace(old_quorum, new_quorum)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated quorum block")
