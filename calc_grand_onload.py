import re

with open('templates/program_mechanic/quote_form.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("document.addEventListener('DOMContentLoaded', function() {", "document.addEventListener('DOMContentLoaded', function() {\n    calcGrand();")

with open('templates/program_mechanic/quote_form.html', 'w', encoding='utf-8') as f:
    f.write(content)
