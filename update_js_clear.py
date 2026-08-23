import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# I will replace if(!reg) return; with if(!reg) return; document.getElementById('tracker-input').value = '';
content = content.replace("if(!reg) return;", "if(!reg) return;\n      document.getElementById('tracker-input').value = '';")

with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
    f.write(content)
