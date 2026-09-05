import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

text = text.replace('SACE Auditor Program', 'Provider Auditor Program')
text = text.replace('Launch Full SACE Program', 'Launch Activity')

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
