with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Update Tile 2
old_tile2_locked = """<!-- Tile 2: Manual Capture -->
<div class="block rounded-xl border p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed">
<div class="font-semibold text-slate-900">2. Manual Bill Capture</div>
<div class="mt-1 text-sm text-slate-600">Locked. You must complete the Setup step first.</div>
</div>"""

new_tile2_locked = """<!-- Tile 2: Architecture Setup -->
<div class="block rounded-xl border p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed">
<div class="font-semibold text-slate-900">2. Architecture Setup</div>
<div class="mt-1 text-sm text-slate-600">Locked. You must complete the Property Setup step first.</div>
</div>"""
content = content.replace(old_tile2_locked, new_tile2_locked)

old_tile2_active = """<a href="{{ url_for('billing_bp.manual_capture') }}" class="block rounded-xl border p-6 shadow-sm transition hover:shadow bg-blue-50 border-blue-100 hover:bg-blue-100 group">
<div class="font-semibold text-slate-900 group-hover:text-blue-700">2. Manual Bill Capture</div>
<div class="mt-1 text-sm text-slate-600">Enter statements, meters, and readings manually.</div>
</a>"""

new_tile2_active = """<a href="{{ url_for('billing_bp.manual_capture') }}" class="block rounded-xl border p-6 shadow-sm transition hover:shadow bg-blue-50 border-blue-100 hover:bg-blue-100 group relative">
<div class="font-semibold text-slate-900 group-hover:text-blue-700">2. Architecture Setup</div>
<div class="mt-1 text-sm text-slate-600">Map out your municipal accounts and meter numbers.</div>
{% if draft_property.onboarding_status == 'draft_manual' %}
<span class="absolute top-4 right-4 flex h-3 w-3"><span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75"></span><span class="relative inline-flex rounded-full h-3 w-3 bg-blue-500"></span></span>
{% endif %}
</a>"""
content = content.replace(old_tile2_active, new_tile2_active)

# Add Tile 3 for Data Capture right after Tile 2
old_tile3 = """<!-- Tile 3: AI PDF Upload -->"""

new_tile3 = """<!-- Tile 2.5: Initial Data Capture -->
{% if draft_property and draft_property.onboarding_status in ['draft_readings'] %}
<a href="{{ url_for('billing_bp.capture_readings') }}" class="block rounded-xl border p-6 shadow-sm transition hover:shadow bg-blue-50 border-blue-100 hover:bg-blue-100 group relative">
<div class="font-semibold text-slate-900 group-hover:text-blue-700">3. Initial Data Capture</div>
<div class="mt-1 text-sm text-slate-600">Enter initial rates, valuation, and meter readings.</div>
<span class="absolute top-4 right-4 flex h-3 w-3"><span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75"></span><span class="relative inline-flex rounded-full h-3 w-3 bg-blue-500"></span></span>
</a>
{% else %}
<div class="block rounded-xl border p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed">
<div class="font-semibold text-slate-900">3. Initial Data Capture</div>
<div class="mt-1 text-sm text-slate-600">Locked. You must map your architecture first.</div>
</div>
{% endif %}

<!-- Tile 3: AI PDF Upload -->"""
content = content.replace(old_tile3, new_tile3)

with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated manager_dashboard.html")
