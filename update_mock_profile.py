import re

with open('app/program_debtors/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("terms_and_conditions = shop.terms_and_conditions", "terms_and_conditions = shop.terms_and_conditions\n                    bank_details = shop.bank_details")

with open('app/program_debtors/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
