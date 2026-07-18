import re

with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_html = """            <div class="mb-4">
              <label class="block text-sm font-bold text-slate-700 mb-2">How many Statement(s) are required?</label>
<input type="number" name="tenants" min="1" value="1" required class="w-full border-2 border-slate-400 rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:outline-none">
            </div>
            <div class="mb-4">
              <label class="block text-sm font-bold text-slate-700 mb-2">Is this a Bulk Metered property?</label>"""

new_html = """            <div class="mb-4">
              <label class="block text-sm font-bold text-slate-700 mb-2">How many Statement(s) are required?</label>
<input type="number" name="tenants" min="1" value="1" required class="w-full border-2 border-slate-400 rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:outline-none">
            </div>
            <div class="mb-4">
              <label class="block text-sm font-bold text-slate-700 mb-2">Total Water Meters</label>
<input type="number" name="expected_water_meters" min="0" value="0" required class="w-full border-2 border-slate-400 rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:outline-none">
            </div>
            <div class="mb-4">
              <label class="block text-sm font-bold text-slate-700 mb-2">Total Electrical Meters</label>
<input type="number" name="expected_elec_meters" min="0" value="0" required class="w-full border-2 border-slate-400 rounded-lg px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:outline-none">
            </div>
            <div class="mb-4">
              <label class="block text-sm font-bold text-slate-700 mb-2">Is this a Bulk Metered property?</label>"""

content = content.replace(old_html, new_html)

with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated manager_dashboard.html initial setup")
