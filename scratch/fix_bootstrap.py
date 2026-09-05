import re

with open('scripts/bootstrap_manor_gardens.py', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('first_name=du["name"].split()[0], last_name=du["name"].split()[1]', 'name=du["name"]')

with open('scripts/bootstrap_manor_gardens.py', 'w', encoding='utf-8') as f:
    f.write(text)
