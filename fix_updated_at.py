import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace job.updated_at with job.completed_at
content = content.replace("job.updated_at", "job.completed_at")

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
