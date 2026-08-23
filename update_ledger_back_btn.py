import re

with open('templates/program_mechanic/client_ledger.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("url_for('mechanic_bp.client_accounts')", "url_for('mechanic_bp.job_cards_list')")

with open('templates/program_mechanic/client_ledger.html', 'w', encoding='utf-8') as f:
    f.write(content)
