import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("for j in active_jobs:\n                j.status = 'Billed'", "for j in active_jobs:\n                j.status = 'Billed'\n            db.session.commit()")

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
