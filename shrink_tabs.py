import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace button classes
content = content.replace(
    'class="flex items-center gap-2 px-4 py-2 rounded-lg font-bold shadow-sm transition bg-indigo-600 text-white border-2 border-indigo-700"',
    'class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-bold shadow-sm transition bg-indigo-600 text-white border-2 border-indigo-700"'
)
content = content.replace(
    'class="flex items-center gap-2 px-4 py-2 rounded-lg font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200"',
    'class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200"'
)

# Also fix the JS that resets the classes
old_js_active = 'activeBtn.className = "flex items-center gap-2 px-4 py-2 rounded-lg font-bold shadow-sm transition bg-indigo-600 text-white border-2 border-indigo-700";'
new_js_active = 'activeBtn.className = "flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-bold shadow-sm transition bg-indigo-600 text-white border-2 border-indigo-700";'
content = content.replace(old_js_active, new_js_active)

old_js_inactive = 'btn.className = "flex items-center gap-2 px-4 py-2 rounded-lg font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200";'
new_js_inactive = 'btn.className = "flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-bold shadow-sm transition bg-slate-100 text-slate-600 border-2 border-slate-200 hover:bg-slate-200";'
content = content.replace(old_js_inactive, new_js_inactive)

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
