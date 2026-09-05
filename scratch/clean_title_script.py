import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Remove the script block inside block title
pattern = r'{% block title %}LiTRE Facilitator Dashboard.*?{% endblock %}'
text = re.sub(pattern, '{% block title %}LiTRE Facilitator Dashboard{% endblock %}', text, flags=re.DOTALL)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
