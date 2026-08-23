import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the redirect to ledger with a redirect to job card detail
content = content.replace("return redirect(url_for('mechanic_bp.client_ledger', debtor_id=debtor.id))", "return redirect(url_for('mechanic_bp.job_card_detail', id=job_card.id))")

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
