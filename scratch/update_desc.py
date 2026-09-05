import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_desc = '<p class="text-xs text-slate-500 mt-1">SACE-appointed Evaluators and Endorsement Committee Members.</p>'
new_desc = '<p class="text-xs text-slate-500 mt-1">SACE-appointed Auditors (e.g. Evaluators and Endorsement Committee Members).</p>'

text = text.replace(old_desc, new_desc)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
