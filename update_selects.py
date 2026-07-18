with open('templates/admin/modules_control.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('border border-slate-300', 'border-2 border-slate-400 outline-none focus:border-indigo-500 focus:ring focus:ring-indigo-200')
content = content.replace('<select name="visibility_{{ s }}"', '<select name="visibility_{{ s }}" {% if loop.first %}autofocus{% endif %}')

with open('templates/admin/modules_control.html', 'w', encoding='utf-8') as f:
    f.write(content)
