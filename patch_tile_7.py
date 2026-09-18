with open("templates/program_uip/dashboards/secretary_workspace.html", "r", encoding="utf-8") as f:
    text = f.read()

# I used "Ratepayer Queries" and "Correspondences" as Tile 6 and 7.
# Let's change Correspondences to point to the Onboarding Campaign.
old_tile = """        <!-- Tile 7: Correspondences -->
        <a href="#" class="group block relative overflow-hidden rounded-2xl border bg-amber-50 border-amber-100 hover:border-amber-300 hover:shadow-md transition-all duration-300">
            <div class="p-6">
                <div class="flex justify-between items-start mb-6">
                    <div class="w-12 h-12 rounded-full flex items-center justify-center text-xl bg-amber-100 text-amber-500">
                        <i class="fas fa-envelope-open-text"></i>
                    </div>
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-[10px] font-bold bg-white text-amber-400 border border-amber-100 uppercase tracking-widest">
                        Comms
                    </span>
                </div>
                <h2 class="text-xl font-black text-amber-900 mb-1">Correspondences</h2>
                <p class="text-sm text-amber-600/70 font-medium">General inbox and official outbox communications.</p>
            </div>
            <div class="h-1.5 w-full bg-amber-400 absolute bottom-0 left-0 opacity-0 group-hover:opacity-100 transition-opacity"></div>
        </a>"""

new_tile = """        <!-- Tile 7: Onboarding Campaign -->
        <a href="{{ url_for('uip_bp.onboarding_campaign', org_slug=org.slug) }}" class="group block relative overflow-hidden rounded-2xl border bg-amber-50 border-amber-100 hover:border-amber-300 hover:shadow-md transition-all duration-300">
            <div class="p-6">
                <div class="flex justify-between items-start mb-6">
                    <div class="w-12 h-12 rounded-full flex items-center justify-center text-xl bg-amber-100 text-amber-500">
                        <i class="fas fa-bullhorn"></i>
                    </div>
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-[10px] font-bold bg-white text-amber-400 border border-amber-100 uppercase tracking-widest animate-pulse">
                        Campaign
                    </span>
                </div>
                <h2 class="text-xl font-black text-amber-900 mb-1">Onboarding Campaign</h2>
                <p class="text-sm text-amber-600/70 font-medium">Manage the 3-Wave Drip emails to activate the precinct.</p>
            </div>
            <div class="h-1.5 w-full bg-amber-400 absolute bottom-0 left-0 opacity-0 group-hover:opacity-100 transition-opacity"></div>
        </a>"""

text = text.replace(old_tile, new_tile)

with open("templates/program_uip/dashboards/secretary_workspace.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated switchboard link for Tile 7")
