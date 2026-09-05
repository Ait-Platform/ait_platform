import re

file_path = 'templates/program_sace/presentation_ppp.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Build array for 1.png to 30.png
slides_js = "const slides = [\n"
for i in range(1, 31):
    slides_js += f"        '{{{{ url_for(\\'static\\', filename=\\'sace_slides/{i}.png\\') }}}}',\n"
slides_js += "    ];"

# We need to find the block of slides array to replace.
pattern = r'const slides = \[.*?\];'
text = re.sub(pattern, slides_js, text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

