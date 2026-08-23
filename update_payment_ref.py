import re

files_to_update = [
    'templates/program_mechanic/public_job_card.html',
    'templates/program_debtors/soa_template.html',
    'templates/program_mechanic/job_card.html'
]

for file in files_to_update:
    with open(file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # We want to replace "Job Card #{{ job_card.job_number }}" and similar things inside the payment reference section.
    content = content.replace(
        "Job Card #{{ job_card.job_number }}",
        "{{ job_card.job_number.split('-')[-1] }}"
    )
    content = content.replace(
        "Job Card #{{ latest_job_card.job_number }}",
        "{{ latest_job_card.job_number.split('-')[-1] }}"
    )
    
    with open(file, 'w', encoding='utf-8') as f:
        f.write(content)
