with open("templates/program_uip/dashboards/resident.html", "r", encoding="utf-8") as f:
    text = f.read()

# Change Ratepayer Dashboard to Owner Dashboard
text = text.replace("Ratepayer Dashboard", "Owner Dashboard")

# Remove the "Switch Participant Role" button since it acts as a back button they didn't like
switch_btn = """<div class="mt-4 md:mt-0 flex flex-col items-end">
            <a href="{{ url_for('uip_bp.router_page', org_slug=org.slug) }}" class="ui-btn ui-btn-outline mb-2">
                &larr; Switch Participant Role
            </a>
            <p class="text-sm text-slate-500">Contact UIP reception to log a new interaction.</p>
        </div>"""

new_action = """<div class="mt-4 md:mt-0 flex flex-col items-end justify-center">
            <button onclick="alert('Fault logging system coming soon!')" class="px-5 py-2.5 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-sm transition flex items-center">
                <i class="fas fa-plus-circle mr-2"></i> Log New Query / Fault
            </button>
        </div>"""

if switch_btn in text:
    text = text.replace(switch_btn, new_action)

# Add Property Details section
quick_links_start = """<div>
            <h2 class="text-xl font-bold text-slate-900 mb-4">Quick Links</h2>"""

property_details = """
        <div>
            <h2 class="text-xl font-bold text-slate-900 mb-4">My Property</h2>
            <div class="bg-indigo-50 rounded-xl border border-indigo-100 p-5 shadow-sm mb-8">
                <div class="flex items-start mb-3">
                    <div class="w-10 h-10 rounded-full bg-indigo-100 text-indigo-600 flex items-center justify-center mr-3 shrink-0 font-bold">
                        <i class="fas fa-home"></i>
                    </div>
                    <div>
                        <h3 class="font-bold text-indigo-900">Registered Ratepayer</h3>
                        <p class="text-sm text-indigo-700">Verified via UIP Vault</p>
                    </div>
                </div>
                <div class="space-y-2 mt-4 text-sm text-indigo-800">
                    <div class="flex justify-between border-b border-indigo-100 pb-2">
                        <span class="font-bold">Status</span>
                        <span class="bg-indigo-600 text-white text-[10px] uppercase tracking-widest px-2 py-0.5 rounded-full font-bold">Active</span>
                    </div>
                    <div class="flex justify-between pt-1">
                        <span class="font-bold">Member Since</span>
                        <span>{{ current_user.created_at.strftime('%Y') }}</span>
                    </div>
                </div>
            </div>

            <h2 class="text-xl font-bold text-slate-900 mb-4">Quick Links</h2>"""

text = text.replace(quick_links_start, property_details)

with open("templates/program_uip/dashboards/resident.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched resident.html")
