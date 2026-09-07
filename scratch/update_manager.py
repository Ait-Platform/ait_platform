import re

file_path = 'templates/uip/dashboards/manager.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('Levy Paid Up', 'Verified Resident')

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

