import re

with open('templates/program_mechanic/client_ledger.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Insert {% block flashes %}{% endblock %} right after {% block content %}
content = content.replace("{% block content %}", "{% block content %}\n{% block flashes %}{% endblock %}")

with open('templates/program_mechanic/client_ledger.html', 'w', encoding='utf-8') as f:
    f.write(content)
