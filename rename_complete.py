import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace Complete with Billed
content = content.replace('<i class="fas fa-check-double mr-1"></i> Complete', '<i class="fas fa-check-double mr-1"></i> Billed')

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
