import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Let's just use regex to clean up any "}</span> of 30;" block
text = re.sub(r'\}</span> of 30;\s*\}', r'}', text)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
