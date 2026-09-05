file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('id="f-counter-global">', 'id="f-counter-global"></span> of 30;')

import re
text = re.sub(r'\n\s*\}\n\s*\}\n\n\s*// Update F Slides', r'\n        }\n\n        // Update F Slides', text)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
