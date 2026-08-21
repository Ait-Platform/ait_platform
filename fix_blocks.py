import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix block title
content = re.sub(r'{% block title %}.*?{% endblock %}', 
                 r'{% block title %}ProTrade - Job Card #{{ job_card.job_number }}{% endblock %}', 
                 content, flags=re.DOTALL)

# Fix block flashes if it has garbage
content = re.sub(r'{% block flashes %}.*?{% endblock %}', 
                 r'{% block flashes %}\n  {% include "partials/_flashes.html" ignore missing %}\n{% endblock %}', 
                 content, flags=re.DOTALL)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)

