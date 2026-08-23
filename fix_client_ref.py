import re

with open('app/program_debtors/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    'client = debtor.mechanic_client',
    '''from app.models.mechanic import MechClient
        client = MechClient.query.get(debtor.reference_id)'''
)

with open('app/program_debtors/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
