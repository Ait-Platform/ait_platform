import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_text = '<p class="text-sm text-slate-600 mb-6">These rows are strictly locked based on the Total Water and Total Electric meters defined in your Property Map.</p>'
new_text = '<p class="text-sm text-slate-600 mb-6">These rows are strictly locked based on the Total Water and Total Electric meters defined in your Property Map. <strong class="text-blue-700 block mt-1"><svg class="w-4 h-4 inline-block mb-0.5 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>Logic: Total Meters - Master Bulk Meter = Remaining Sub-Meters</strong></p>'

if old_text in content:
    content = content.replace(old_text, new_text)
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Fixed Step 5 text description.")
else:
    print("Could not find the old text to replace.")
