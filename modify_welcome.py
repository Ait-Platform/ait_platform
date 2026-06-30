import re

with open('templates/public/welcome.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('Mechanic CRM', 'Mechanic Customer Relationship Management')
content = content.replace('Medical Practice', 'Health Care')

with open('templates/public/welcome.html', 'w', encoding='utf-8') as f:
    f.write(content)
