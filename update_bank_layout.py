import re

files_to_update = [
    'templates/program_mechanic/public_job_card.html',
    'templates/program_mechanic/job_card.html',
    'templates/program_debtors/soa_template.html'
]

for file in files_to_update:
    with open(file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # In public_job_card and soa_template:
    content = content.replace(
        '<div><strong>BSB:</strong> {{ bank_account.bsb_branch }} &nbsp; <strong>Account No:</strong> {{ bank_account.account_number }}</div>',
        '<div><strong>BSB:</strong> {{ bank_account.bsb_branch }}</div>\\n<div><strong>Account No:</strong> {{ bank_account.account_number }}</div>'
    )
    # In job_card:
    content = content.replace(
        '<p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Account No:</span> {{ bank_account.account_number }}</p>\\n                    <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">BSB:</span> {{ bank_account.bsb_branch }}</p>',
        '<p class="text-slate-600 text-sm mt-1"><span class="font-semibold">BSB:</span> {{ bank_account.bsb_branch }}</p>\\n                    <p class="text-slate-600 text-sm mt-1"><span class="font-semibold">Account No:</span> {{ bank_account.account_number }}</p>'
    )
    
    with open(file, 'w', encoding='utf-8') as f:
        f.write(content)
