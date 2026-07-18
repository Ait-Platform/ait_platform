import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_badge_block = """        <div class="flex space-x-2 text-[11px]">
          <span id="step-1-badge" class="px-2 py-1 rounded-full font-bold bg-blue-100 text-blue-700">1. Property</span>
          <span id="step-2-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">2. Accounts</span>
          <span id="step-3-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">3. Bulk Water</span>
          <span id="step-4-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">4. Bulk Elec</span>
          <span id="step-5-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">5. Sub Meters</span>
          <span id="step-6-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">6. Exceptions</span>
          <span id="step-7-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">7. Mapping</span>
        </div>"""

new_badge_block = """        <div class="flex space-x-2 text-[11px]">
          <span id="step-1-badge" class="px-2 py-1 rounded-full font-bold bg-blue-100 text-blue-700">1. Property</span>
          <span id="step-2-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">2. Accounts</span>
          <span id="step-3-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">3. Bulk Water</span>
          <span id="step-4-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">4. Bulk Elec</span>
          <span id="step-5-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">5. Sub Meters</span>
          <span id="step-6-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">6. Exceptions</span>
          <span id="step-7-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">7. Mapping</span>
          <span id="step-8-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">8. Arrears</span>
          <span id="step-9-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">9. Arrangements</span>
          <span id="step-10-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">10. Owners</span>
        </div>"""

if old_badge_block in content:
    content = content.replace(old_badge_block, new_badge_block)
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Fixed badges HTML.")
else:
    print("Could not find old badges.")

# I also need to make sure updateBadges doesn't crash on labels length.
old_labels = "const labels = ['1. Property', '2. Accounts', '3. Bulk Water', '4. Bulk Elec', '5. Sub Meters', '6. Exceptions', '7. Mapping'];"
new_labels = "const labels = ['1. Property', '2. Accounts', '3. Bulk Water', '4. Bulk Elec', '5. Sub Meters', '6. Exceptions', '7. Mapping', '8. Arrears', '9. Arrangements', '10. Owners'];"

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

if old_labels in content:
    content = content.replace(old_labels, new_labels)
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Fixed badge labels in JS.")

