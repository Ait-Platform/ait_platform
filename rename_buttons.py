import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('Accept Quote', 'Mark as Accepted')
content = content.replace('Reject Quote', 'Mark as Rejected')

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
