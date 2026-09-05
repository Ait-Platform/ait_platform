import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Fix the backslash typo
text = text.replace("showTab(\\'f\\')", "showTab('f')")
text = text.replace("showTab(\\'p\\')", "showTab('p')")

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
