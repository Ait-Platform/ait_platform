import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    'bank_account = BusinessBankAccount.query.filter_by(user_id=job_card.vehicle.client.user_id, is_default=True).first()',
    'bank_account = BusinessBankAccount.query.filter_by(user_id=job_card.vehicle.client.user_id).order_by(BusinessBankAccount.is_default.desc()).first()'
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
