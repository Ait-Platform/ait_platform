import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("import pytz", "")

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
