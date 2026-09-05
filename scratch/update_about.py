import re

file_path = 'templates/program_sace/about.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('SACE Activity Workshop', 'AIT Provider Portfolio: SACE Endorsement Review')

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
