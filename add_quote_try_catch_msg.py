import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    'flash(f"An error occurred while saving the quote: {str(e).split(\'DETAIL:\')[0].strip()}", "danger")',
    'flash(f"An error occurred while saving the quote. Your tokens were refunded. Error: {str(e).split(\'DETAIL:\')[0].strip()[:100]}", "danger")'
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
