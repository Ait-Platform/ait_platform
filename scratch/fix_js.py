import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

bad_js = 'document.getElementById(\'step-counter-container\').innerHTML = Step <span id="f-counter-global"></span> of 30;'
good_js = 'document.getElementById(\'step-counter-container\').innerHTML = Step <span id="f-counter-global"></span> of 30;'

text = text.replace(bad_js, good_js)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
