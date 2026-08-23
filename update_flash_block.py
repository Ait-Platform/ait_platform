import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

if '{% block flashes %}' not in content:
    content = content.replace(
        '{% block content %}',
        '{% block flashes %}{% endblock %}\n\n{% block content %}'
    )

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
