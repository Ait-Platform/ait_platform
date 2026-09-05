import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

pattern = r'@sace_bp\.route\("/sace/reading/presentation"\)\n@login_required\n+@sace_bp\.route\("/sace/acknowledge_patent"'
replacement = r'@sace_bp.route("/sace/acknowledge_patent"'
text = re.sub(pattern, replacement, text)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
