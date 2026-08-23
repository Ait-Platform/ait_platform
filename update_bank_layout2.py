import re

files_to_update = [
    'templates/program_mechanic/public_job_card.html',
    'templates/program_mechanic/job_card.html',
    'templates/program_debtors/soa_template.html'
]

for file in files_to_update:
    with open(file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    content = content.replace(
        '<div><strong>BSB:</strong> {{ bank_account.bsb_branch }}</div>\\n<div><strong>Account No:</strong> {{ bank_account.account_number }}</div>',
        '<div><strong>BSB:</strong> {{ bank_account.bsb_branch }}</div>\n                            <div><strong>Account No:</strong> {{ bank_account.account_number }}</div>'
    )
    
    with open(file, 'w', encoding='utf-8') as f:
        f.write(content)
