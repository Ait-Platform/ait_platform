import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

regex = r'(@mechanic_bp\.route\("/mechanic/client_ledger/<int:debtor_id>/add_payment", methods=\["POST"\]\).*?def client_ledger_add_payment\(debtor_id\):.*?)return redirect\(url_for\(\'mechanic_bp\.job_card_detail\', id=job_card\.id\)\)'
replacement = r"\1return redirect(url_for('mechanic_bp.client_ledger', debtor_id=debtor.id))"

content = re.sub(regex, replacement, content, flags=re.DOTALL)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
