import re

for filename in ['templates/program_mechanic/job_cards_list.html', 'templates/program_mechanic/email_preview.html']:
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
    
    if '{% block flashes %}' not in content:
        content = content.replace('{% block content %}', '{% block flashes %}{% endblock %}\n\n{% block content %}')
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
            print(f"Added block flashes to {filename}")
