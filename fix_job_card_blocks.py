import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''{% block title %}ProTrade - Job Card #{{ job_card.job_number }}
<script>''',
    '''{% block title %}ProTrade - Job Card #{{ job_card.job_number }}{% endblock %}
{% block content %}
<script>'''
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
