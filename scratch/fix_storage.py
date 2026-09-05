import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("sessionStorage.getItem('sace_joined')", "localStorage.getItem('sace_joined')")
content = content.replace("sessionStorage.removeItem('sace_joined')", "localStorage.removeItem('sace_joined')")

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)

with open('templates/program_sace/participant_join.html', 'r', encoding='utf-8') as f:
    content2 = f.read()

content2 = content2.replace("sessionStorage.setItem('sace_joined', 'true')", "localStorage.setItem('sace_joined', 'true')")

with open('templates/program_sace/participant_join.html', 'w', encoding='utf-8') as f:
    f.write(content2)
