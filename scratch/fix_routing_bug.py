import re

files = [
    'templates/program_sace/presentation_ppp.html',
    'templates/program_sace/simulator.html',
    'templates/program_sace/post_test/results.html'
]

for file_path in files:
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
    
    text = text.replace("url_for('sace_bp.reading_index')", "url_for('sace_bp.reading_hub')")
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(text)
