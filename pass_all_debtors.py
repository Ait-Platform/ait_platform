import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

regex = r'total_debtors_count=total_debtors_count\)'
replacement = r'total_debtors_count=total_debtors_count, all_debtors=all_debtors if "all_debtors" in locals() else [])'

content = re.sub(regex, replacement, content)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
