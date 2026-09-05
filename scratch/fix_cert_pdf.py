import re

file_path = 'templates/program_sace/post_test/certificate_pdf.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Fix the Jinja loop to safely handle missing competencies
old_loop = r'\{% for comp in answers\.competencies %\}'
new_loop = r'{% for comp in answers.get("competencies", []) %}'

text = re.sub(old_loop, new_loop, text)

# Fix the score check
old_score = r'\{% if answers and answers\.score is defined %\}'
new_score = r'{% if answers and answers.get("score") is not none %}'
text = re.sub(old_score, new_score, text)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
