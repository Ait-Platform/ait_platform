import re

# 1. public_job_card.html
with open('templates/program_mechanic/public_job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('<div><strong>Bank:</strong> {{ bank_account.bank_name }}</div>', '<div>Bank: <strong>{{ bank_account.bank_name }}</strong></div>')
content = content.replace('<div><strong>Account Name:</strong> {{ bank_account.account_name }}</div>', '<div>Account Name: <strong>{{ bank_account.account_name }}</strong></div>')
content = content.replace('<div><strong>BSB:</strong> {{ bank_account.bsb_branch }}</div>', '<div>BSB: <strong>{{ bank_account.bsb_branch }}</strong></div>')
content = content.replace('<div><strong>Account No:</strong> {{ bank_account.account_number }}</div>', '<div>Account No: <strong>{{ bank_account.account_number }}</strong></div>')
content = content.replace('<div><strong>SWIFT:</strong> {{ bank_account.swift_code }}</div>', '<div>SWIFT: <strong>{{ bank_account.swift_code }}</strong></div>')
content = content.replace('<strong>Payment Reference:</strong><br>{{ job_card.job_number.split(\'-\')[-1] }}', 'Payment Reference:<br><strong>{{ job_card.job_number.split(\'-\')[-1] }}</strong>')

with open('templates/program_mechanic/public_job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)


# 2. soa_template.html
with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('<div><strong>Bank:</strong> {{ bank_account.bank_name }}</div>', '<div>Bank: <strong>{{ bank_account.bank_name }}</strong></div>')
content = content.replace('<div><strong>Account Name:</strong> {{ bank_account.account_name }}</div>', '<div>Account Name: <strong>{{ bank_account.account_name }}</strong></div>')
content = content.replace('<div><strong>BSB:</strong> {{ bank_account.bsb_branch }}</div>', '<div>BSB: <strong>{{ bank_account.bsb_branch }}</strong></div>')
content = content.replace('<div><strong>Account No:</strong> {{ bank_account.account_number }}</div>', '<div>Account No: <strong>{{ bank_account.account_number }}</strong></div>')
content = content.replace('<div><strong>SWIFT:</strong> {{ bank_account.swift_code }}</div>', '<div>SWIFT: <strong>{{ bank_account.swift_code }}</strong></div>')
content = content.replace('<strong>Payment Reference:</strong><br>{% if latest_job_card %}{{ latest_job_card.job_number.split(\'-\')[-1] }}{% else %}Account: {{ debtor.name }}{% endif %}', 'Payment Reference:<br><strong>{% if latest_job_card %}{{ latest_job_card.job_number.split(\'-\')[-1] }}{% else %}{{ debtor.name }}{% endif %}</strong>')

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)


# 3. job_card.html
with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('<span class="font-semibold">Bank:</span> {{ bank_account.bank_name }}', 'Bank: <span class="font-bold text-slate-900">{{ bank_account.bank_name }}</span>')
content = content.replace('<span class="font-semibold">Account:</span> {{ bank_account.account_name }}', 'Account Name: <span class="font-bold text-slate-900">{{ bank_account.account_name }}</span>')
content = content.replace('<span class="font-semibold">BSB:</span> {{ bank_account.bsb_branch }}', 'BSB: <span class="font-bold text-slate-900">{{ bank_account.bsb_branch }}</span>')
content = content.replace('<span class="font-semibold">Account No:</span> {{ bank_account.account_number }}', 'Account No: <span class="font-bold text-slate-900">{{ bank_account.account_number }}</span>')
content = content.replace('<span class="font-semibold">Payment Reference:</span><br>{{ job_card.job_number.split(\'-\')[-1] }}', 'Payment Reference:<br><span class="font-bold text-slate-900">{{ job_card.job_number.split(\'-\')[-1] }}</span>')

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)

