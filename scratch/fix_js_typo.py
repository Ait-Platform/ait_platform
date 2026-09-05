import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

bad_js = '</span> of 30;\\n\\n        // Update F Slides'
good_js = '\\n\\n        // Update F Slides'

text = text.replace(bad_js, good_js)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
