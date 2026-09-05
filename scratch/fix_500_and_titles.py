import re

file_path = 'templates/program_sace/reading_hub.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# 1. Extract the progress calculation
calc_block = '''            {% set total = 7 %}
            {% set completed = 0 %}
            {% if progress.app_form %}{% set completed = completed + 1 %}{% endif %}
            {% if progress.reviewer_guide %}{% set completed = completed + 1 %}{% endif %}
            {% if progress.patent %}{% set completed = completed + 1 %}{% endif %}
            {% if progress.annexures %}{% set completed = completed + 1 %}{% endif %}
            {% if progress.ppp %}{% set completed = completed + 1 %}{% endif %}
            {% if progress.demo_cert %}{% set completed = completed + 1 %}{% endif %}
            {% if progress.reading_cert %}{% set completed = completed + 1 %}{% endif %}
            {% set percent = (completed / total * 100) | round | int %}'''

# Remove it from where it is
text = text.replace(calc_block, '')

# Inject it right after {% block content %}
content_marker = '{% block content %}'
text = text.replace(content_marker, content_marker + '\n' + calc_block)

# 2. Change reading_hub.html title
text = text.replace("Provider's SACE Activities", "Sace Authorised User(s) Map")

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)

# 3. Change compliance/index.html title
comp_path = 'templates/program_sace/compliance/index.html'
with open(comp_path, 'r', encoding='utf-8') as f: comp_text = f.read()
comp_text = comp_text.replace('Sace Authorised User(s) Map', 'Provider Activities')
with open(comp_path, 'w', encoding='utf-8') as f: f.write(comp_text)
