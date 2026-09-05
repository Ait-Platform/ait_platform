import re
file_path = 'templates/program_sace/post_test/results.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

text = text.replace("url_for('sace_bp.post_test')", "url_for('sace_bp.post_test', retake=1)")

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
