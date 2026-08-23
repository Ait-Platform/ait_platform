import re

with open('app/admin/general/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''core_modules = ['practice_crm', 'cultural_fire', 'debtors', 'mechanic']''',
    '''core_modules = ['practice_crm', 'cultural_fire', 'debtors', 'mechanic', 'budget', 'billing']'''
)

with open('app/admin/general/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
