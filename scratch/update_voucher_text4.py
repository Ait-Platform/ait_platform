import re

with open('templates/auth/checkout_decision.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    'Are you attending a pre-paid SACE CPTD Approved Activity?',
    'Are you attending a SACE CPTD Approved Activity?'
)

with open('templates/auth/checkout_decision.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Removed 'pre-paid' from the text")
