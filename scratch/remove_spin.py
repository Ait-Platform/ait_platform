import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Remove fa-spin
text = text.replace('fa-sync fa-spin', 'fa-sync')

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
