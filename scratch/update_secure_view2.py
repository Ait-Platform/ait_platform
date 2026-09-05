import re
file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

pattern = r"    doc_url = url_for\('static', filename=doc\.file_path\.replace\('app/static/', ''\)\.replace\('static/', ''\)\)"
replacement = '''    if doc and doc.file_path:
        doc_url = url_for('static', filename=doc.file_path.replace('app/static/', '').replace('static/', ''))
    elif not doc_url:
        doc_url = ""'''
text = re.sub(pattern, replacement, text)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
