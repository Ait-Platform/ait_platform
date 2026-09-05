import re

with open('templates/program_sace/participant_join.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("'/sace/workshop/interactive'", "'{{ url_for('sace_bp.interactive_workshop') }}'")

with open('templates/program_sace/participant_join.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Fixed redirect URL")
