with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Fix the active controls box text
text = text.replace('text-indigo-300', 'text-indigo-900')
text = text.replace('text-slate-300', 'text-indigo-700')

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
