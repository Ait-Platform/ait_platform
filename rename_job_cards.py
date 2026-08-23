import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Change title
content = content.replace("Recent Job Cards", "Job Cards Dashboard")

# Change tabs text
content = content.replace("Pending<br>Quotes", "Pending<br>Job Cards (PJC)")
content = content.replace("Accepted /<br>In Progress", "Confirmed<br>Job Cards (CJC)")

# Change table headers
content = content.replace("Pending Quotes", "Pending Job Cards (PJC)")
content = content.replace("Accepted / In Progress", "Confirmed Job Cards (CJC)")

# Change button from 'Accept' to 'Confirm'
content = content.replace('<i class="fas fa-check mr-1"></i> Accept', '<i class="fas fa-check mr-1"></i> Confirm')
content = content.replace('title="Mark as Accepted"', 'title="Mark as Confirmed"')

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
