import re

html_path = 'templates/program_sace/post_test/test.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('LITRE Blending Machine', 'I Learn to Read English Using the LITRE Method')
text = text.replace('LITRE Reading Workshop', 'I Learn to Read English Using the LITRE Method')

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
