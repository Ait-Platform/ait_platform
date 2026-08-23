import re

with open('app/program_budget/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''flash("Payment added. 10 tokens deducted.", "success")''',
    '''flash(f"Payment added. {token_cost} tokens deducted.", "success")'''
)

with open('app/program_budget/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
