import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Append Payment reference right before the </div> that closes the Banking Details group.
# Look for the last {% endif %} in the group
regex = r'(\{% else %\}.*?No bank details configured.*?\{% endif %\})'

content = re.sub(regex, r'\1\n            <p class="text-slate-600 text-sm mt-3"><span class="font-semibold">Payment Reference:</span><br>{{ job_card.job_number.split(\'-\')[-1] }}</p>', content, flags=re.DOTALL)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
