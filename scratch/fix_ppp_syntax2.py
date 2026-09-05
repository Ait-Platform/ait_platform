file_path = 'templates/program_sace/presentation_ppp.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Replace the specific syntax issue
text = text.replace('{{ url_for(\\\'static\\\', filename=\\\'sace_slides/', "{{ url_for('static', filename='sace_slides/")
text = text.replace('.png\\\') }}', ".png') }}")

# Ensure no weird escaping remains
text = text.replace('{ img: "{{ url_for(\\\'static\\\', filename=\\\'sace_slides/', '{ img: "{{ url_for(\'static\', filename=\'sace_slides/')
text = text.replace('.png\\\') }}" },', '.png\') }}" },')

# The current text actually looks like: { img: "{{ url_for(\'static\', filename=\'sace_slides/1.png\') }}" },
# We want: { img: "{{ url_for('static', filename='sace_slides/1.png') }}" },
text = text.replace("\\'", "'")

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
