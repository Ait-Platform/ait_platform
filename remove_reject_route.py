import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

regex = r'@mechanic_bp\.route\("/mechanic/job_card/<int:id>/reject", methods=\["POST"\]\)\s*@login_required\s*def reject_quote\(id\):.*?(?=@mechanic_bp\.route|\Z)'

content = re.sub(regex, '', content, flags=re.DOTALL)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
