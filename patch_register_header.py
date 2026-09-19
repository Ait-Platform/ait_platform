with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    text = f.read()

old_header = """<header class="ui-page-head flex justify-between items-start mb-8 border-b border-slate-200 pb-6">
    <div>
        {% if current_appointment and current_appointment.position == "Secretary" %}
        <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}" class="inline-flex items-center text-sm font-bold text-indigo-600 hover:text-indigo-800 mb-3 bg-indigo-50 px-3 py-1 rounded-full transition">
            <i class="fas fa-arrow-left mr-2"></i> Back to Secretary Dashboard
        </a>
        {% endif %}
        <h1 class="text-3xl font-extrabold text-slate-900 mt-1">Resolution Register</h1>
        <p class="text-slate-600 mt-2">Official ledger of all drafted, proposed, and adopted resolutions.</p>
    </div>
    <div class="mt-1">
        <span class="bg-indigo-100 text-indigo-800 text-xs px-3 py-1.5 rounded-full font-bold tracking-wide shadow-sm border border-indigo-200">
            <i class="fas fa-university mr-1"></i> {{ org.name }}
        </span>
    </div>
</header>"""

new_header = """<header class="ui-page-head mb-8 border-b border-slate-200 pb-6">
    <!-- Row 1: Title & Back Button -->
    <div class="flex justify-between items-center mb-2">
        <h1 class="text-3xl font-extrabold text-slate-900 tracking-tight">Resolution Register</h1>
        {% if current_appointment and current_appointment.position == "Secretary" %}
        <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}" class="inline-flex items-center text-sm font-bold text-slate-500 hover:text-indigo-600 transition">
            <i class="fas fa-arrow-left mr-2"></i> Back to Secretary Dashboard
        </a>
        {% endif %}
    </div>
    <!-- Row 2: Subtitle & Action Buttons -->
    <div class="flex justify-between items-end">
        <p class="text-slate-500 font-medium max-w-2xl">Official ledger of all drafted, proposed, and adopted resolutions.</p>
        <div class="flex gap-3">
            <button class="px-4 py-2 bg-white border border-slate-200 hover:bg-slate-50 hover:border-slate-300 text-slate-700 text-sm font-bold rounded-lg shadow-sm transition"><i class="fas fa-eye mr-2"></i> View Log</button>
            <button class="px-4 py-2 bg-white border border-slate-200 hover:bg-slate-50 hover:border-slate-300 text-slate-700 text-sm font-bold rounded-lg shadow-sm transition"><i class="fas fa-envelope mr-2"></i> Email Register</button>
            <a href="{{ url_for('uip_bp.draft_resolution', org_slug=org.slug) }}" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-bold rounded-lg shadow-sm transition inline-flex items-center">
                <i class="fas fa-plus mr-2"></i> Create Resolution
            </a>
        </div>
    </div>
</header>"""

if old_header in text:
    text = text.replace(old_header, new_header)
    with open("templates/program_uip/dashboards/committee.html", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched committee.html")
else:
    print("Could not find header in committee.html")
