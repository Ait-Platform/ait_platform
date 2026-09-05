import re

with open('templates/program_sace/reading_hub.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("url_for('sace.reviewer_guide')", "url_for('sace_bp.reviewer_guide')")

with open('templates/program_sace/reading_hub.html', 'w', encoding='utf-8') as f:
    f.write(content)
