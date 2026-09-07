import re

file_path = 'templates/uip/dashboards/manager.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('Resident & Levy Status', 'Resident Status')

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
