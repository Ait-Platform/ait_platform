import re
with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    text = f.read()

pattern = re.compile(r'<header class="ui-page-head flex justify-between items-start mb-8 border-b border-slate-200 pb-6">.*?</header>', re.DOTALL)

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

text = pattern.sub(new_header, text, count=1)
with open("templates/program_uip/dashboards/committee.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched committee.html via regex")
