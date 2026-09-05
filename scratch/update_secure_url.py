import re
file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

text = text.replace('doc_url = ""', 'doc_url = "about:blank"')

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
