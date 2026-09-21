with open("templates/program_uip/dashboards/ratepayer_workspace.html", "r", encoding="utf-8") as f:
    html = f.read()

old_header = """    <div class="mb-12 border-b border-slate-200 pb-6">
        <h1 class="text-4xl font-extrabold text-slate-900 tracking-tight">Ratepayer Dashboard</h1>
        <p class="text-slate-500 mt-2 font-medium">Manage your property, report issues, and vote on community mandates.</p>
    </div>"""

new_header = """    <div class="mb-12 border-b border-slate-200 pb-6 flex justify-between items-end">
        <div>
            <h1 class="text-4xl font-extrabold text-slate-900 tracking-tight">Ratepayer Dashboard</h1>
            <p class="text-slate-500 mt-2 font-medium">Manage your property, report issues, and vote on community mandates.</p>
        </div>
        <div class="text-right">
            <p class="text-indigo-600 font-bold text-lg mb-1">{{ current_user.name }}</p>
            <span class="inline-flex items-center px-3 py-1 rounded-full bg-slate-100 text-slate-600 border border-slate-200 text-xs font-bold uppercase tracking-wider">
                <i class="fas fa-home mr-2"></i> Verified Ratepayer
            </span>
        </div>
    </div>"""

html = html.replace(old_header, new_header)

with open("templates/program_uip/dashboards/ratepayer_workspace.html", "w", encoding="utf-8") as f:
    f.write(html)
print("Updated header for ratepayer workspace")
