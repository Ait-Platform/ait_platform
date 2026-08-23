import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '{% block content %}',
    '{% block content %}\n{% set currency_sym = "R " %}'
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
