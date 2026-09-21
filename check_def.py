with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()
import re
for match in re.finditer(r'def .*?\(.*?\):', text):
    if 'claim' in match.group(0).lower():
        print(match.group(0))
