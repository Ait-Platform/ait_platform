with open('templates/program_mechanic/price.html', 'r', encoding='utf-8') as f:
    content = f.read()
    
content = content.replace('{% include "partials/flash_messages.html" %}', '')

with open('templates/program_mechanic/price.html', 'w', encoding='utf-8') as f:
    f.write(content)
