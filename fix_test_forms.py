import os
import re

d = r'D:\Users\yeshk\Documents\ait_platform\templates\school_home'
for f in os.listdir(d):
    if f.startswith('test_') and f.endswith('.html'):
        p = os.path.join(d, f)
        with open(p, 'r', encoding='utf-8') as file:
            content = file.read()
            
        modified = False
        
        # Remove {{ form.hidden_tag() }} and any optional trailing comments on that line
        if '{{ form.hidden_tag() }}' in content:
            content = re.sub(r'\{\{\s*form\.hidden_tag\(\)\s*\}\}.*\n?', '', content)
            modified = True
            
        if modified:
            with open(p, 'w', encoding='utf-8') as file:
                file.write(content)

print("Removed undefined 'form.hidden_tag()' from all test files.")
