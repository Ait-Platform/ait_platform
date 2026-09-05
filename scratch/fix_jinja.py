import re

with open('templates/program_sace/sace_catalog.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the escaped quotes
content = content.replace("\\'", "'")

with open('templates/program_sace/sace_catalog.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Fixed Jinja syntax error")
