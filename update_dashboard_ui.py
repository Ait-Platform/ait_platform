import re

with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_html = """          <div>
            <label class="block text-sm font-semibold text-slate-700 mb-1">Statement(s)</label>
            <input type="number" name="expected_tenants" value="{{ draft_property.expected_tenants }}" min="1" required class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500">
          </div>
        </div>
        <div class="grid grid-cols-2 gap-4">"""

new_html = """          <div>
            <label class="block text-sm font-semibold text-slate-700 mb-1">Statement(s)</label>
            <input type="number" name="expected_tenants" value="{{ draft_property.expected_tenants }}" min="1" required class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500">
          </div>
        </div>
        <div class="grid grid-cols-2 gap-4">
          <div>
            <label class="block text-sm font-semibold text-slate-700 mb-1">Total Water Meters</label>
            <input type="number" name="expected_water_meters" value="{{ draft_property.expected_water_meters|default(0) }}" min="0" required class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500">
          </div>
          <div>
            <label class="block text-sm font-semibold text-slate-700 mb-1">Total Elec Meters</label>
            <input type="number" name="expected_elec_meters" value="{{ draft_property.expected_elec_meters|default(0) }}" min="0" required class="w-full rounded border-2 border-slate-300 px-3 py-2 outline-none focus:border-blue-500">
          </div>
        </div>
        <div class="grid grid-cols-2 gap-4 mt-4">"""

content = content.replace(old_html, new_html)
with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated manager_dashboard.html")
