import re

file_path = 'templates/program_sace/post_test/certificate_pdf.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('Sace approved activity', 'Sace activity')
text = text.replace('SACE APPROVED ACTIVITY', 'SACE ACTIVITY')

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
