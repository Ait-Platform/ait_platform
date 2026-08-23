import re

with open('templates/program_mechanic/client_accounts.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('shop.shop_name', 'shop.business_name')

with open('templates/program_mechanic/client_accounts.html', 'w', encoding='utf-8') as f:
    f.write(content)
