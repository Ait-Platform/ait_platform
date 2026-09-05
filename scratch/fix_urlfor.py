import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace("url_for('core_bp.dashboard')", "url_for('sace_bp.dashboard')")

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
