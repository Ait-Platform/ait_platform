import re

filepath = 'templates/program_uip/register_import.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search_code = '''{% block content %}
<header class="ui-header-2row">'''

replace_code = '''{% block content %}
<style>
    .ui-sidebar { display: none !important; }
    .ui-shell { grid-template-columns: 1fr !important; display: block !important; }
    .ui-workspace { padding-left: 0 !important; margin-left: 0 !important; max-width: 1200px; margin: 0 auto !important; width: 100%; }
</style>
<header class="ui-header-2row">'''

content = content.replace(search_code, replace_code)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Added full-width style to register_import.html")
