import re

with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace Tile 4 and Tile 5 logic
target = '''          <!-- Tile 4: Enter Readings (MetSOA) -->
          <div class="block rounded-xl border p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed">
            <div class="font-semibold text-slate-900">4. Enter Readings</div>
            <div class="mt-1 text-sm text-slate-600">Locked until collation is complete.</div>
          </div>

          <!-- Tile 5: Financial Requirements -->
          <div class="block rounded-xl border p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed">
            <div class="font-semibold text-slate-900">5. Financial Requirements</div>
            <div class="mt-1 text-sm text-slate-600">Locked until readings are finalized.</div>
          </div>'''

injection = '''          <!-- Tile 4: Enter Readings (MetSOA) -->
          {% if draft_property and draft_property.onboarding_status in ['draft_readings', 'draft_financials'] %}
          <a href="{{ url_for('billing_bp.input_readings', property_id=draft_property.id) }}" class="block rounded-xl border p-6 shadow-sm transition hover:shadow bg-orange-50 border-orange-100 hover:bg-orange-100 group">
            <div class="font-semibold text-slate-900 group-hover:text-orange-700">4. Enter Readings</div>
            <div class="mt-1 text-sm text-slate-600">Enter meter readings manually or by AI.</div>
          </a>
          {% else %}
          <div class="block rounded-xl border p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed">
            <div class="font-semibold text-slate-900">4. Enter Readings</div>
            <div class="mt-1 text-sm text-slate-600">Locked until collation is complete.</div>
          </div>
          {% endif %}

          <!-- Tile 5: Financial Requirements -->
          {% if draft_property and draft_property.onboarding_status == 'draft_financials' %}
          <a href="{{ url_for('billing_bp.edit_property', property_id=draft_property.id) }}?onboarding=complete" class="block rounded-xl border p-6 shadow-sm transition hover:shadow bg-pink-50 border-pink-100 hover:bg-pink-100 group">
            <div class="font-semibold text-slate-900 group-hover:text-pink-700">5. Financial Requirements</div>
            <div class="mt-1 text-sm text-slate-600">Setup leases, deposits, and target recovery.</div>
          </a>
          {% else %}
          <div class="block rounded-xl border p-6 shadow-sm bg-slate-50 border-slate-200 opacity-75 cursor-not-allowed">
            <div class="font-semibold text-slate-900">5. Financial Requirements</div>
            <div class="mt-1 text-sm text-slate-600">Locked until readings are finalized.</div>
          </div>
          {% endif %}'''

content = content.replace(target, injection)

with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
