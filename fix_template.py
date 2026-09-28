import re

filepath = 'templates/program_uip/register_import.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("{% extends 'layouts/uip_base.html' %}", "{% extends 'program_uip/base.html' %}")

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
