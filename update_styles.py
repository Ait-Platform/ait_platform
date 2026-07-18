with open('templates/admin/settings.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('border border-slate-300', 'border-2 border-slate-400 outline-none')

with open('templates/admin/settings.html', 'w', encoding='utf-8') as f:
    f.write(content)
