import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

regex = r'(debtors_with_balances\.append\(d\)\s*except Exception as e:\s*current_app\.logger\.error\(f"Error loading debtors: \{e\}"\)\s*)(return render_template\("program_mechanic/job_cards_list\.html", job_cards=job_cards, debtors_with_balances=debtors_with_balances\))'

def replacer(match):
    return match.group(1) + 'total_debtors_count = len(all_debtors) if "all_debtors" in locals() else 0\n    ' + match.group(2).replace(')', ', total_debtors_count=total_debtors_count)')

content = re.sub(regex, replacer, content)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
