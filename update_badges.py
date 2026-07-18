import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

badge_logic = """  function getAccountBadge(acc) {
      if (!IS_BULK_WATER && !IS_BULK_ELEC) return ''; 
      if (acc.isBulk) return '<span class="text-[10px] bg-amber-200 text-amber-800 px-2 py-0.5 rounded ml-2 font-bold uppercase border border-amber-300 shadow-sm">Bulk</span>';
      return '<span class="text-[10px] bg-sky-200 text-sky-800 px-2 py-0.5 rounded ml-2 font-bold uppercase border border-sky-300 shadow-sm">Sub</span>';
  }
  
  function nextStep() {"""

if 'function getAccountBadge' not in html:
    html = html.replace('  function nextStep() {', badge_logic)

# Replace all occurrences of the hardcoded badges
pattern1 = r"\$\{acc\.isBulk \? '<span class=\"text-\[10px\] bg-amber-200 text-amber-800 px-2 py-0\.5 rounded ml-2 font-bold uppercase border border-amber-300 shadow-sm\">Bulk</span>' : '<span class=\"text-\[10px\] bg-sky-200 text-sky-800 px-2 py-0\.5 rounded ml-2 font-bold uppercase border border-sky-300 shadow-sm\">Sub</span>'\}"
html = re.sub(pattern1, "${getAccountBadge(acc)}", html)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(html)
print("Updated badge logic successfully")
