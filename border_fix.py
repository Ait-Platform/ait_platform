with open('templates/program_mechanic/price.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    'class="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-800 focus:outline-none focus:ring-2 focus:ring-slate-200"',
    'class="w-full rounded-lg border-2 border-indigo-400 bg-white px-3 py-2 text-sm text-slate-800 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"'
)

with open('templates/program_mechanic/price.html', 'w', encoding='utf-8') as f:
    f.write(content)
