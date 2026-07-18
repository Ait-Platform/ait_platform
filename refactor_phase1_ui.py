with open('templates/program_billing/manual_capture.html', 'r', encoding='utf-8') as f:
    content = f.read()

import re

# 1. Update titles and instructions
content = content.replace('Municipal Accounts Setup', 'Phase 1: Architecture Setup')
content = content.replace('Please set up each of your {{ property.expected_bills }} municipal accounts below. You can save partial progress and come back at any time.', 'Please map out each of your {{ property.expected_bills }} municipal accounts below. Define the account numbers and their physical meters. No readings are required at this stage.')

# 2. Update the Tabs Navigation to remove Financials
tabs_old = """      <!-- Tabs Navigation -->
      <div class="flex border-b border-slate-200 px-6 bg-white">
        <button type="button" onclick="switchTab('tab-main')" id="btn-tab-main" class="px-4 py-3 font-bold text-sm border-b-2 border-blue-600 text-blue-700 focus:outline-none">1. Main Details</button>
        <button type="button" onclick="switchTab('tab-financials')" id="btn-tab-financials" class="px-4 py-3 font-bold text-sm border-b-2 border-transparent text-slate-500 hover:text-slate-700 focus:outline-none">2. Rates & Arrears</button>
        <button type="button" onclick="switchTab('tab-meters')" id="btn-tab-meters" class="px-4 py-3 font-bold text-sm border-b-2 border-transparent text-slate-500 hover:text-slate-700 focus:outline-none">3. Meters & Readings</button>
      </div>"""

tabs_new = """      <!-- Tabs Navigation -->
      <div class="flex border-b border-slate-200 px-6 bg-white">
        <button type="button" onclick="switchTab('tab-main')" id="btn-tab-main" class="px-4 py-3 font-bold text-sm border-b-2 border-blue-600 text-blue-700 focus:outline-none">1. Account Details</button>
        <button type="button" onclick="switchTab('tab-meters')" id="btn-tab-meters" class="px-4 py-3 font-bold text-sm border-b-2 border-transparent text-slate-500 hover:text-slate-700 focus:outline-none">2. Physical Meters</button>
      </div>"""
content = content.replace(tabs_old, tabs_new)

# 3. Remove the entire tab-financials div
financials_regex = r'<!-- TAB 2: Financials -->\s*<div id="tab-financials" class="wizard-tab hidden space-y-6">.*?</div>\s*</div>\s*<!-- TAB 3: Meters -->'
content = re.sub(financials_regex, '<!-- TAB 2: Meters -->', content, flags=re.DOTALL)

# 4. Remove readings inputs from the meter template
readings_regex = r'<div class="grid grid-cols-2 md:grid-cols-4 gap-3 bg-slate-50 p-3 rounded-lg border border-slate-200">.*?</div>\s*</div>\s*</template>'
new_readings_end = """</div>\n</template>"""
content = re.sub(readings_regex, new_readings_end, content, flags=re.DOTALL)

# 5. Fix JS switchTab references
js_tabs_old = "const tabs = ['tab-main', 'tab-financials', 'tab-meters'];"
js_tabs_new = "const tabs = ['tab-main', 'tab-meters'];"
content = content.replace(js_tabs_old, js_tabs_new)

# 6. Fix JS packing logic to ignore readings
js_pack_old = """        curr_date: card.querySelector('.m-cdate').value,
        prev_read: card.querySelector('.m-pread').value,
        curr_read: card.querySelector('.m-cread').value"""
js_pack_new = """        // No readings packed during Phase 1"""
content = content.replace("        prev_date: card.querySelector('.m-pdate').value,", "// No dates packed")
content = content.replace(js_pack_old, js_pack_new)

with open('templates/program_billing/manual_capture.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated manual_capture.html for Phase 1.")
