import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

regex = r'<!-- DEBTORS WITH BALANCES -->.*?\{% endif %\}'

content = re.sub(regex, '', content, flags=re.DOTALL)

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
