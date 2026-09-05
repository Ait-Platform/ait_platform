import re
file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('Step <span id="f-counter-global">0</span> of 25', 'Step <span id="f-counter-global">0</span> of 28')

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
