import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("url_for('debtors_bp.debtors_dashboard')", "url_for('debtors_bp.dashboard')")

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
