import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("<!-- <!-- Floating SACE Guide Button -->", "<!-- Floating SACE Guide Button (HIDDEN) ")
content = content.replace("</button> -->\n\n<!-- SACE Guide Modal -->", "</button> -->\n\n<!-- SACE Guide Modal -->")

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
