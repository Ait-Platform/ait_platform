with open("templates/program_uip/dashboards/ratepayer_workspace.html", "r", encoding="utf-8") as f:
    html = f.read()

# Replace Pillar 1 content
old_pillar = """        <!-- Pillar 1: Public Voting Room -->
        <a href="#" class="group block relative overflow-hidden rounded-2xl border bg-white border-slate-200 hover:border-indigo-300 hover:shadow-md transition-all duration-300">
            <div class="p-8">
                <div class="flex justify-between items-start mb-6">
                    <div class="w-14 h-14 rounded-full flex items-center justify-center text-2xl bg-indigo-50 text-indigo-600 group-hover:bg-indigo-600 group-hover:text-white transition-colors">
                        <i class="fas fa-vote-yea"></i>
                    </div>
                </div>
                <h2 class="text-2xl font-black text-slate-800 mb-2 group-hover:text-indigo-700 transition-colors">Public Voting Room</h2>
                <p class="text-slate-500 font-medium">Cast your secure digital vote on community mandates and resolutions.</p>
            </div>
            <div class="h-2 w-full bg-indigo-500 absolute bottom-0 left-0 opacity-0 group-hover:opacity-100 transition-opacity"></div>
        </a>"""

new_pillar = """        <!-- Pillar 1: Public Mandates Register -->
        <a href="{{ url_for('uip_bp.public_mandates', org_slug=org.slug) }}" class="group block relative overflow-hidden rounded-2xl border bg-white border-slate-200 hover:border-emerald-300 hover:shadow-md transition-all duration-300">
            <div class="p-8">
                <div class="flex justify-between items-start mb-6">
                    <div class="w-14 h-14 rounded-full flex items-center justify-center text-2xl bg-emerald-50 text-emerald-600 group-hover:bg-emerald-600 group-hover:text-white transition-colors">
                        <i class="fas fa-landmark"></i>
                    </div>
                </div>
                <h2 class="text-2xl font-black text-slate-800 mb-2 group-hover:text-emerald-700 transition-colors">Public Mandates Register</h2>
                <p class="text-slate-500 font-medium">View the official, read-only ledger of all legally adopted resolutions.</p>
            </div>
            <div class="h-2 w-full bg-emerald-500 absolute bottom-0 left-0 opacity-0 group-hover:opacity-100 transition-opacity"></div>
        </a>"""

html = html.replace(old_pillar, new_pillar)

with open("templates/program_uip/dashboards/ratepayer_workspace.html", "w", encoding="utf-8") as f:
    f.write(html)
print("Updated ratepayer workspace to point to public mandates")
