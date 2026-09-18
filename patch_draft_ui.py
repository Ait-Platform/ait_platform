import re
with open("templates/program_uip/dashboards/resolution_draft.html", "r", encoding="utf-8") as f:
    text = f.read()

# Update Title input
old_title = """        <div>
            <label class="block text-sm font-bold text-slate-700 mb-2">Resolution Title</label>
            <input type="text" name="title" required class="w-full p-4 border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 text-lg font-bold" placeholder="e.g. Appointment of Security Contractor">
        </div>"""

new_title = """        <div>
            <label class="block text-sm font-bold text-slate-700 mb-2">Resolution Category / Title</label>
            <input type="text" name="title" list="title-suggestions" required class="w-full p-4 border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 text-lg font-bold" placeholder="Select a category or type a custom title...">
            <datalist id="title-suggestions">
                <option value="General Resolution">
                <option value="Financial Authorization">
                <option value="Service Provider Appointment">
                <option value="Policy Adoption">
                <option value="Emergency Action">
            </datalist>
            <p class="text-xs text-slate-400 mt-2">You can select a standard category from the dropdown or type your own specific title.</p>
        </div>"""
text = text.replace(old_title, new_title)

# Update Quorum input
old_quorum = """            <div>
                <label class="block text-sm font-bold text-slate-700 mb-2">Quorum Target (%)</label>
                <input type="number" name="quorum_target" min="1" max="100" value="50" class="w-full p-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
            </div>"""

new_quorum = """            <div>
                <label class="block text-sm font-bold text-slate-700 mb-2">Quorum Target (%)</label>
                <input type="number" name="quorum_target" min="1" max="100" value="50" class="w-full p-3 border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500">
                <p class="text-[10px] text-amber-700 mt-2 font-bold leading-tight bg-amber-50 p-2 rounded border border-amber-100">
                    <i class="fas fa-info-circle mr-1"></i> Ensure this complies with the Companies Act and Precinct Constitution (Standard ExCo decisions typically require a minimum 50% quorum).
                </p>
            </div>"""
text = text.replace(old_quorum, new_quorum)

with open("templates/program_uip/dashboards/resolution_draft.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated resolution_draft.html")
