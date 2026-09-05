import re
file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('Guide (A)', 'SACE Auditor (A)')

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
