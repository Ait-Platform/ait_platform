import re

with open('app/program_debtors/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# I will replace "use_custom_letterhead = shop.use_custom_letterhead" with "use_custom_letterhead = shop.use_custom_letterhead\n                    terms_and_conditions = shop.terms_and_conditions"
content = content.replace('use_custom_letterhead = shop.use_custom_letterhead', 'use_custom_letterhead = shop.use_custom_letterhead\n                    terms_and_conditions = shop.terms_and_conditions')

with open('app/program_debtors/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
