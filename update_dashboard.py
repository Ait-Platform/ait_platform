with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

import re

# We will replace the "Tile 2: View Extraction" block with the new layout
old_block_pattern = r'<!-- Tile 2: View Extraction -->[\s\S]*?{% endif %}'

new_block = """<!-- Tile 2A: Manual Bill Capture -->
{% if draft_property and draft_property.onboarding_status in ['draft_extracting', 'draft_collating', 'draft_manual'] %}
<a href="{{ url_for('billing_bp.manual_capture') }}?property_id={{ draft_property.id }}" class="block rounded-xl border p-6 shadow-sm transition hover:shadow bg-emerald-50 border-emerald-100 hover:bg-emerald-100 group">
  <div class="font-semibold text-slate-900 group-hover:text-emerald-700">2. Manual Bill Capture</div>
  <div class="mt-1 text-sm text-slate-600">Manually capture bill details (Owner, Account, Readings, Rates).</div>
</a>
{% else %}
<div class="block rounded-xl border p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed">
  <div class="font-semibold text-slate-900">2. Manual Bill Capture</div>
  <div class="mt-1 text-sm text-slate-600">Locked until a property setup is initiated.</div>
</div>
{% endif %}

<!-- Tile 2B: AI Extraction (Temporarily Blocked) -->
<div class="block rounded-xl border p-6 shadow-sm bg-slate-50 border-slate-200 opacity-50 cursor-not-allowed relative overflow-hidden">
  <div class="absolute top-2 right-2 bg-amber-100 text-amber-800 text-xs font-bold px-2 py-1 rounded">Blocked (Fine-tuning)</div>
  <div class="font-semibold text-slate-900">2B. AI View Extraction</div>
  <div class="mt-1 text-sm text-slate-600">AI extraction is temporarily disabled for maintenance and fine-tuning.</div>
</div>"""

if re.search(old_block_pattern, content):
    content = re.sub(old_block_pattern, new_block, content)
    with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Dashboard updated successfully.")
else:
    print("Pattern not found.")
