import re

file_path = 'templates/program_sace/presentation_ppp.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

text = re.sub(r"{ img: '\{\{ url_for\(\\'static\\', filename=\\'sace_slides/(\d+)\.png\\'\) \}\}' \},", 
              r'{ img: "{{ url_for(\'static\', filename=\'sace_slides/\1.png\') }}" },', text)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
