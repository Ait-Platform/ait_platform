with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

old_header = """    <header class="ui-page-head flex justify-between items-start mb-8 border-b border-slate-200 pb-6">
        <div>
            <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}" class="inline-flex items-center text-sm font-bold text-indigo-600 hover:text-indigo-800 mb-3 bg-indigo-50 px-3 py-1 rounded-full transition">
                <i class="fas fa-arrow-left mr-2"></i> Back to Secretary Dashboard
            </a>
            <h1 class="text-3xl font-extrabold text-slate-900 mt-1">Organogram Builder</h1>
            <p class="text-slate-600 mt-2">Map the organizational blueprint, define seats, and upload official photos.</p>
        </div>
        <div class="mt-1 flex gap-3">
            <button onclick="document.getElementById('addSeatModal').classList.remove('hidden')" class="ui-btn ui-btn-primary shadow-sm">
                <i class="fas fa-plus mr-2"></i> Add New Seat
            </button>
        </div>
    </header>"""

new_header = """    <header class="ui-page-head mb-8 border-b border-slate-200 pb-6">
        <!-- Row 1: Title & Back Button -->
        <div class="flex justify-between items-center mb-2">
            <h1 class="text-3xl font-extrabold text-slate-900 tracking-tight">Organogram Builder</h1>
            <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}" class="inline-flex items-center text-sm font-bold text-slate-500 hover:text-indigo-600 transition">
                <i class="fas fa-arrow-left mr-2"></i> Back to Secretary Dashboard
            </a>
        </div>
        <!-- Row 2: Subtitle & Action Buttons -->
        <div class="flex justify-between items-end">
            <p class="text-slate-500 font-medium max-w-2xl">Map the organizational blueprint, define seats, and upload official photos.</p>
            <div class="flex gap-3">
                <button class="px-4 py-2 bg-white border border-slate-200 hover:bg-slate-50 hover:border-slate-300 text-slate-700 text-sm font-bold rounded-lg shadow-sm transition"><i class="fas fa-eye mr-2"></i> View</button>
                <button class="px-4 py-2 bg-white border border-slate-200 hover:bg-slate-50 hover:border-slate-300 text-slate-700 text-sm font-bold rounded-lg shadow-sm transition"><i class="fas fa-envelope mr-2"></i> Email</button>
                <button onclick="document.getElementById('addSeatModal').classList.remove('hidden')" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-bold rounded-lg shadow-sm transition">
                    <i class="fas fa-plus mr-2"></i> Add New Seat
                </button>
            </div>
        </div>
    </header>"""

if old_header in text:
    text = text.replace(old_header, new_header)
    with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched Organogram Header successfully.")
else:
    print("Failed to find the old header in the file.")
