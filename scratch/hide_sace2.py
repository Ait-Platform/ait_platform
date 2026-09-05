import re

file_path = 'templates/public/programs.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_hide = '''{% if slug not in ["admin", "admin_general", "sms", "spv"] %}'''
new_hide = '''{% if slug not in ["admin", "admin_general", "sms", "spv", "sace_cptd", "sace_evaluator", "sace_facilitator", "sace_participant", "staff"] %}'''

text = text.replace(old_hide, new_hide)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
