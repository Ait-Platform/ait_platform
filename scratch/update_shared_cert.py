import re

file_path = 'templates/shared/ait_certificate_layout.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Replace "AIT Platform" with "Archoney Institute of Technology" or "AIT"
text = text.replace('<div class="header-title">AIT Platform</div>', '<div class="header-title">AIT</div>')
text = text.replace('<div>AIT Platform</div>', '<div>AIT</div>')
text = text.replace('within the AIT Platform.', 'with AIT.')

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
