import re

files = [
    "templates/program_uip/dashboards/secretary_organogram.html",
    "templates/program_uip/dashboards/secretary_intake.html",
    "templates/program_uip/dashboards/committee.html"
]

for file in files:
    with open(file, "r", encoding="utf-8") as f:
        text = f.read()

    # Replacements
    text = text.replace('<header class="mb-8 border-b border-slate-200 pb-6" style="width: 100%; display: flex; flex-direction: column; gap: 0.5rem;">', '<header class="ui-header-2row">')
    text = text.replace('<div style="display: flex; justify-content: space-between; align-items: center; width: 100%;">', '<div class="ui-header-2row-top">')
    text = text.replace('<div style="display: flex; justify-content: space-between; align-items: flex-end; width: 100%;">', '<div class="ui-header-2row-bottom">')
    text = text.replace('class="text-3xl font-extrabold text-slate-900 tracking-tight" style="margin: 0;"', 'class="text-3xl font-extrabold text-slate-900 tracking-tight ui-header-2row-title"')
    text = text.replace('class="text-slate-500 font-medium max-w-2xl" style="margin: 0;"', 'class="text-slate-500 font-medium ui-header-2row-subtitle"')
    text = text.replace('<div style="display: flex; gap: 0.75rem; justify-content: flex-end;">', '<div class="ui-header-actions">')
    
    with open(file, "w", encoding="utf-8") as f:
        f.write(text)

print("Applied ui.css classes to HTML templates")
