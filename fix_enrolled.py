import os
import re

d = r'D:\Users\yeshk\Documents\ait_platform\templates\school_home'
for f in os.listdir(d):
    if f.startswith('chapter_') and f.endswith('.html'):
        p = os.path.join(d, f)
        with open(p, 'r', encoding='utf-8') as file:
            content = file.read()
            
        modified = False
        
        # We want to replace any {% if ... session.get('enrolled... %}
        # with {% if 'home' in session.get('enrolled_subjects', []) %}
        new_content = re.sub(
            r'\{%\s*if[^%]*session\.get\([\'"]enrolled[^\}]+%\}',
            r'{% if \'home\' in session.get(\'enrolled_subjects\', []) %}',
            content
        )
        
        if new_content != content:
            with open(p, 'w', encoding='utf-8') as file:
                file.write(new_content)

print("Updated all session.get('enrolled...') conditions in chapter files.")
