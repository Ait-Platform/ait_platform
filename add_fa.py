import re

with open('templates/layout.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '<!-- Tailwind (swap for compiled CSS in prod) -->',
    '<!-- FontAwesome for Icons -->\n  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">\n  <!-- Tailwind (swap for compiled CSS in prod) -->'
)

with open('templates/layout.html', 'w', encoding='utf-8') as f:
    f.write(content)
