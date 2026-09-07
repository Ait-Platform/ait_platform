import re

init_path = 'app/uip/__init__.py'
with open(init_path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace("manager_role = CoreRole(name='Manager', slug='manager', access_level=10)", "manager_role = CoreRole(name='Manager', slug='manager')")

with open(init_path, 'w', encoding='utf-8') as f:
    f.write(text)
