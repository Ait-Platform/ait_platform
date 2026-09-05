import re

with open('templates/program_sace/participant_join.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('{% extends "base.html" %}', '{% extends "layout.html" %}')
content = content.replace('{% block content %}', '{% block title %}Join Workshop{% endblock %}\n{% block content %}')

with open('templates/program_sace/participant_join.html', 'w', encoding='utf-8') as f:
    f.write(content)
