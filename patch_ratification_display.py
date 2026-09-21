with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    html = f.read()

inject_target = "{% include \"partials/flash_messages.html\" %}"

ratification_block = """
    {% if resolution.status in ['ADOPTED', 'REJECTED'] and resolution.result_basis and resolution.result_basis.get('ratification') %}
    {% set rat = resolution.result_basis.ratification %}
    <div class="mb-6 bg-slate-50 border border-slate-200 rounded-xl p-6 shadow-sm">
        <h3 class="font-bold text-slate-800 mb-4 text-lg"><i class="fas fa-file-contract text-slate-500 mr-2"></i> Official Ratification Record</h3>
        <div class="grid grid-cols-2 md:grid-cols-4 gap-6">
            <div>
                <div class="text-[10px] uppercase tracking-widest font-bold text-slate-400 mb-1">Live Meeting Date</div>
                <div class="font-black text-slate-800">{{ rat.date }}</div>
            </div>
            <div>
                <div class="text-[10px] uppercase tracking-widest font-bold text-slate-400 mb-1">Location</div>
                <div class="font-bold text-slate-700">{{ rat.location }}</div>
            </div>
            <div>
                <div class="text-[10px] uppercase tracking-widest font-bold text-slate-400 mb-1">Live Vote Tally</div>
                <div class="font-bold text-slate-700 text-sm">
                    <span class="text-emerald-600">Y: {{ rat.votes_yea }}</span> &middot; 
                    <span class="text-rose-600">N: {{ rat.votes_nay }}</span> &middot; 
                    <span class="text-amber-600">A: {{ rat.votes_abstain }}</span>
                </div>
            </div>
            <div>
                <div class="text-[10px] uppercase tracking-widest font-bold text-slate-400 mb-1">Recorded By</div>
                <div class="font-bold text-slate-700">{{ rat.recorded_by_name }}</div>
                <div class="text-xs text-slate-500">{{ rat.recorded_by_email }}</div>
            </div>
        </div>
    </div>
    {% endif %}
"""

html = html.replace(inject_target, inject_target + "\n" + ratification_block)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(html)
print("Injected ratification record banner")
