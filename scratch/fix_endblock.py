import re
html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('{% block title %}SACE Control Centre', '{% block title %}SACE Control Centre{% endblock %}')

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
