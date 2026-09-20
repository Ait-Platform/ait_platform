with open("templates/program_uip/dashboards/public_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

# Add a Print button to the header of public organogram
old_header = """        <p class="text-xl text-slate-500 mt-2 max-w-2xl mx-auto">Official Community Organogram & Operations Blueprint</p>
    </div>"""

new_header = """        <p class="text-xl text-slate-500 mt-2 max-w-2xl mx-auto">Official Community Organogram & Operations Blueprint</p>
        <div class="mt-8 print:hidden">
            <button onclick="window.print()" class="inline-flex items-center px-6 py-3 bg-slate-800 hover:bg-slate-900 text-white font-bold rounded-xl shadow-sm transition">
                <i class="fas fa-print mr-2"></i> Print Hard Copy
            </button>
        </div>
    </div>"""

text = text.replace(old_header, new_header)

with open("templates/program_uip/dashboards/public_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)

# Also add a link to it from the Secretary Organogram
with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    sec_text = f.read()

old_sec_header = """        <div class="ui-header-actions">
            <button onclick="document.getElementById('addSeatModal').classList.remove('hidden')" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-bold rounded-lg shadow-sm transition" style="white-space: nowrap;">
                <i class="fas fa-plus mr-2"></i> Add New Seat
            </button>
        </div>"""

new_sec_header = """        <div class="ui-header-actions flex space-x-3">
            <a href="{{ url_for('uip_bp.public_organogram', org_slug=org.slug) }}" target="_blank" class="px-4 py-2 bg-white border border-slate-200 text-slate-700 hover:bg-slate-50 text-sm font-bold rounded-lg shadow-sm transition inline-flex items-center" style="white-space: nowrap;">
                <i class="fas fa-print mr-2"></i> Print / Public View
            </a>
            <button onclick="document.getElementById('addSeatModal').classList.remove('hidden')" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-bold rounded-lg shadow-sm transition inline-flex items-center" style="white-space: nowrap;">
                <i class="fas fa-plus mr-2"></i> Add New Seat
            </button>
        </div>"""

sec_text = sec_text.replace(old_sec_header, new_sec_header)

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(sec_text)

print("Added print button")
