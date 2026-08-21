import re

with open('templates/program_debtors/profile.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace border-gray-300 with border-2 border-slate-300
content = content.replace('border border-gray-300', 'border-2 border-slate-300 focus:border-indigo-500')

with open('templates/program_debtors/profile.html', 'w', encoding='utf-8') as f:
    f.write(content)
