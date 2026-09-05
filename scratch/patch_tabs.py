import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Restore the F and P tabs to be clickable buttons, but remove the opacity-50 so they don't look "disabled"
# The current HTML for F tab is:
# <div class="flex items-center px-6 py-3 bg-slate-800 text-slate-500 font-bold rounded-t-lg transition border-b-2 border-transparent opacity-50 cursor-default" id="btn-tab-f">
# Let's replace it with a button that has no opacity-50:
text = re.sub(r'<div class="flex items-center px-6 py-3 bg-slate-800 text-slate-500 font-bold rounded-t-lg transition border-b-2 border-transparent opacity-50 cursor-default" id="btn-tab-f">', 
              r'<button class="flex items-center px-6 py-3 bg-slate-200 text-slate-600 font-bold rounded-t-lg transition border-b-2 border-slate-300 hover:bg-slate-300" id="btn-tab-f" onclick="showTab(\'f\')">', text)

text = re.sub(r'<div class="flex items-center px-6 py-3 bg-slate-800 text-slate-500 font-bold rounded-t-lg transition border-b-2 border-transparent opacity-50 cursor-default" id="btn-tab-p">', 
              r'<button class="flex items-center px-6 py-3 bg-slate-200 text-slate-600 font-bold rounded-t-lg transition border-b-2 border-slate-300 hover:bg-slate-300" id="btn-tab-p" onclick="showTab(\'p\')">', text)

text = text.replace('Facilitator (F)\n            </div>', 'Facilitator (F)\n            </button>')
text = text.replace('Participant (P)\n            </div>', 'Participant (P)\n            </button>')

# 2. Also update JS inactiveClass to not use opacity-50 or dark slate-800
old_inactive = 'const inactiveClass = "flex items-center px-6 py-3 bg-slate-800 text-slate-500 font-bold rounded-t-lg transition border-b-2 border-transparent opacity-50 cursor-default";'
new_inactive = 'const inactiveClass = "flex items-center px-6 py-3 bg-slate-200 text-slate-600 font-bold rounded-t-lg transition border-b-2 border-slate-300 hover:bg-slate-300 cursor-pointer";'
text = text.replace(old_inactive, new_inactive)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
