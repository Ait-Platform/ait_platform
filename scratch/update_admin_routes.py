import re

with open('app/admin/security/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Add zar_amount_cents to the query
content = content.replace(
    'SELECT c.alpha2 as country_code, c.name as country_name, c.currency, p.local_amount_cents',
    'SELECT c.alpha2 as country_code, c.name as country_name, c.currency, p.local_amount_cents, p.zar_amount_cents'
)

with open('app/admin/security/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated admin routes for ZAR cents")
