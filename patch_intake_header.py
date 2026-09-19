with open("templates/program_uip/dashboards/secretary_intake.html", "r", encoding="utf-8") as f:
    text = f.read()

old_header = """<header class="ui-page-head flex justify-between items-start mb-8 border-b border-slate-200 pb-6">
    <div>
        <h1 class="text-3xl font-extrabold text-slate-900 mt-1">Verification of Members</h1>
        <p class="text-sm text-slate-500 mt-2">Review access claims and draft digital resolutions for committee approval.</p>
    </div>
    <div class="mt-1">
        <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}" class="ui-btn ui-btn-outline font-bold">
            &larr; Back to Switchboard
        </a>
    </div>
</header>"""

new_header = """<header class="ui-page-head mb-8 border-b border-slate-200 pb-6">
    <!-- Row 1: Title & Back Button -->
    <div class="flex justify-between items-center mb-2">
        <h1 class="text-3xl font-extrabold text-slate-900 tracking-tight">Verification of Members</h1>
        <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}" class="inline-flex items-center text-sm font-bold text-slate-500 hover:text-indigo-600 transition">
            <i class="fas fa-arrow-left mr-2"></i> Back to Secretary Dashboard
        </a>
    </div>
    <!-- Row 2: Subtitle & Action Buttons -->
    <div class="flex justify-between items-end">
        <p class="text-slate-500 font-medium max-w-2xl">Review access claims and draft digital resolutions for committee approval.</p>
        <div class="flex gap-3">
            <button class="px-4 py-2 bg-white border border-slate-200 hover:bg-slate-50 hover:border-slate-300 text-slate-700 text-sm font-bold rounded-lg shadow-sm transition"><i class="fas fa-eye mr-2"></i> View</button>
            <button class="px-4 py-2 bg-white border border-slate-200 hover:bg-slate-50 hover:border-slate-300 text-slate-700 text-sm font-bold rounded-lg shadow-sm transition"><i class="fas fa-envelope mr-2"></i> Email</button>
            <button class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-bold rounded-lg shadow-sm transition" onclick="document.getElementById('draftResForm').submit()">
                <i class="fas fa-gavel mr-2"></i> Draft Resolution
            </button>
        </div>
    </div>
</header>"""

if old_header in text:
    text = text.replace(old_header, new_header)
    with open("templates/program_uip/dashboards/secretary_intake.html", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched secretary_intake.html")
else:
    print("Could not find header in secretary_intake.html")
