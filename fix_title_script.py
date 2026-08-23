import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the title block
content = content.replace(
    '''{% block title %}ProTrade - Job Card #{{ job_card.job_number }}
<script>''',
    '''{% block title %}ProTrade - Job Card #{{ job_card.job_number }}{% endblock %}
{% block head %}
<script>'''
)

# And replace the endblock of the script so it closes the head block
content = content.replace(
    '''});
</script>
{% endblock %}

{% block flashes %}''',
    '''});
</script>
{% endblock %}

{% block flashes %}'''
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
