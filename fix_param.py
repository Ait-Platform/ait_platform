import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("job_id=job.id", "id=job.id")

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Fixed job_id parameter")
