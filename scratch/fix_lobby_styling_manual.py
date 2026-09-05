with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Just replace the classes manually using regex or simple string replacement on exactly what is there.
text = text.replace('bg-indigo-900/30 border-indigo-500/50', 'bg-indigo-50 border-indigo-200 shadow-sm')
text = text.replace('bg-indigo-900/30 border border-indigo-500/50', 'bg-indigo-50 border border-indigo-200 shadow-sm')
text = text.replace('text-white mb-2 text-lg', 'text-indigo-900 mb-2 text-lg')
text = text.replace('text-indigo-400', 'text-indigo-600')
text = text.replace('text-slate-100 text-base leading-relaxed', 'text-slate-700 text-base leading-relaxed')

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
print("Manually replaced CSS classes")
