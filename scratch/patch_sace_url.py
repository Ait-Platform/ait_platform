import re

with open('templates/program_sace/reading_hub.html', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace("url_for('sace_bp.index')", "url_for('sace_bp.dashboard')")

with open('templates/program_sace/reading_hub.html', 'w', encoding='utf-8') as f:
    f.write(text)
