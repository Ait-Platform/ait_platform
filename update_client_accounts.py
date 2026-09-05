import re

with open('templates/program_mechanic/client_accounts.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("url_for('mechanic_bp.mechanic_dashboard')", "url_for('mechanic_bp.job_cards_list')")

with open('templates/program_mechanic/client_accounts.html', 'w', encoding='utf-8') as f:
    f.write(content)
