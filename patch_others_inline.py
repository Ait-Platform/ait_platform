new_intake_header = """<header class="mb-8 border-b border-slate-200 pb-6" style="width: 100%; display: flex; flex-direction: column; gap: 0.5rem;">
    <!-- Row 1: Title & Back Button -->
    <div style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
        <h1 class="text-3xl font-extrabold text-slate-900 tracking-tight" style="margin: 0;">Verification of Members</h1>
        <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}" class="inline-flex items-center text-sm font-bold text-slate-500 hover:text-indigo-600 transition" style="white-space: nowrap;">
            <i class="fas fa-arrow-left mr-2"></i> Back to Secretary Dashboard
        </a>
    </div>
    <!-- Row 2: Subtitle & Action Buttons -->
    <div style="display: flex; justify-content: space-between; align-items: flex-end; width: 100%;">
        <p class="text-slate-500 font-medium max-w-2xl" style="margin: 0;">Review access claims and draft digital resolutions for committee approval.</p>
        <div style="display: flex; gap: 0.75rem; justify-content: flex-end;">
            <button class="px-4 py-2 bg-white border border-slate-200 hover:bg-slate-50 hover:border-slate-300 text-slate-700 text-sm font-bold rounded-lg shadow-sm transition"><i class="fas fa-eye mr-2"></i> View</button>
            <button class="px-4 py-2 bg-white border border-slate-200 hover:bg-slate-50 hover:border-slate-300 text-slate-700 text-sm font-bold rounded-lg shadow-sm transition"><i class="fas fa-envelope mr-2"></i> Email</button>
            <button onclick="document.getElementById('draftResForm').submit()" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-bold rounded-lg shadow-sm transition" style="white-space: nowrap;">
                <i class="fas fa-gavel mr-2"></i> Draft Resolution
            </button>
        </div>
    </div>
</header>"""

new_register_header = """<header class="mb-8 border-b border-slate-200 pb-6" style="width: 100%; display: flex; flex-direction: column; gap: 0.5rem;">
    <!-- Row 1: Title & Back Button -->
    <div style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
        <h1 class="text-3xl font-extrabold text-slate-900 tracking-tight" style="margin: 0;">Resolution Register</h1>
        {% if current_appointment and current_appointment.position == "Secretary" %}
        <a href="{{ url_for('uip_bp.secretary_workspace', org_slug=org.slug) }}" class="inline-flex items-center text-sm font-bold text-slate-500 hover:text-indigo-600 transition" style="white-space: nowrap;">
            <i class="fas fa-arrow-left mr-2"></i> Back to Secretary Dashboard
        </a>
        {% endif %}
    </div>
    <!-- Row 2: Subtitle & Action Buttons -->
    <div style="display: flex; justify-content: space-between; align-items: flex-end; width: 100%;">
        <p class="text-slate-500 font-medium max-w-2xl" style="margin: 0;">Official ledger of all drafted, proposed, and adopted resolutions.</p>
        <div style="display: flex; gap: 0.75rem; justify-content: flex-end;">
            <button class="px-4 py-2 bg-white border border-slate-200 hover:bg-slate-50 hover:border-slate-300 text-slate-700 text-sm font-bold rounded-lg shadow-sm transition"><i class="fas fa-eye mr-2"></i> View Log</button>
            <button class="px-4 py-2 bg-white border border-slate-200 hover:bg-slate-50 hover:border-slate-300 text-slate-700 text-sm font-bold rounded-lg shadow-sm transition"><i class="fas fa-envelope mr-2"></i> Email Register</button>
            <a href="{{ url_for('uip_bp.draft_resolution', org_slug=org.slug) }}" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-bold rounded-lg shadow-sm transition inline-flex items-center" style="white-space: nowrap;">
                <i class="fas fa-plus mr-2"></i> Create Resolution
            </a>
        </div>
    </div>
</header>"""

import re
# Intake
with open("templates/program_uip/dashboards/secretary_intake.html", "r", encoding="utf-8") as f:
    text = f.read()
pattern = re.compile(r'<header class="mb-8 border-b border-slate-200 pb-6 w-full">.*?</header>', re.DOTALL)
text = pattern.sub(new_intake_header, text, count=1)
with open("templates/program_uip/dashboards/secretary_intake.html", "w", encoding="utf-8") as f:
    f.write(text)

# Register
with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    text = f.read()
text = pattern.sub(new_register_header, text, count=1)
with open("templates/program_uip/dashboards/committee.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Patched intake and register inline css")
