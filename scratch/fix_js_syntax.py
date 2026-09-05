import re

file_path = 'templates/program_sace/post_test/test.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

bad_js1 = r'stepContainer\.querySelector\(input\[name=""\]:checked\)'
good_js1 = r'stepContainer.querySelector(input[name=""]:checked)'
text = re.sub(bad_js1, good_js1, text)

bad_js2 = r'stepContainer\.querySelector\(input\[name=""\]\)'
good_js2 = r'stepContainer.querySelector(input[name=""])'
text = re.sub(bad_js2, good_js2, text)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
