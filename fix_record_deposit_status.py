import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''        job_card.deposit_amount = (job_card.deposit_amount or 0) + deposit_amount
        job_card.status = 'Approved' # Moving from Awaiting Deposit to Approved''',
    '''        job_card.deposit_amount = (job_card.deposit_amount or 0) + deposit_amount
        if job_card.status == 'Awaiting Deposit':
            job_card.status = 'Approved' # Moving forward if it was awaiting'''
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
