import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix doc_type for emailing
content = content.replace(
    'doc_type = "SOA" if job_card.status in [\'Approved\', \'Billed\'] else "Quote"',
    'doc_type = "SOA" if job_card.status == \'Billed\' else ("Tax Invoice" if job_card.status not in [\'Quote\', \'Rejected\'] else "Quote")'
)

# Fix doc_type for downloading
content = content.replace(
    'doc_type = "Invoice" if job_card.status == \'Billed\' else "Quote"',
    'doc_type = "Tax_Invoice" if job_card.status not in [\'Quote\', \'Rejected\'] else "Quote"'
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
