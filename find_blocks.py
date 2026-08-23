with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if '{% endblock %}' in line or '{% block' in line:
        print(f"Line {i+1}: {line.strip()}")
