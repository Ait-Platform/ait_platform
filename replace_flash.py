import os
import glob
files = glob.glob('templates/program_culturefire/*_dashboard.html')
for file in files:
    with open(file, 'r', encoding='utf-8') as f:
        content = f.read()
    new_content = content.replace('{% include "partials/flash_messages.html" %}', '{% include "partials/flash_messages_inline.html" %}')
    new_content = new_content.replace("{% include 'partials/flash_messages.html' %}", '{% include "partials/flash_messages_inline.html" %}')
    if new_content != content:
        with open(file, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f'Updated {file}')
