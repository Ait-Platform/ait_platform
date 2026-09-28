import re

filepath = 'templates/program_uip/operations/issues.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('<p class="ui-eyebrow">Operations</p>', '<p class="ui-eyebrow">Central Inbox</p>')
content = content.replace('<h1>Staff Dashboard</h1>', '<h1>Queries Desk</h1>')
content = content.replace('Track each enquiry from first contact to resolution.', 'Central desk for all ExCo, SubCom, and Staff to track and resolve queries.')

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated UI branding to Queries Desk")
