import os
import re

d = r'D:\Users\yeshk\Documents\ait_platform\templates\school_home'
for f in os.listdir(d):
    if f.startswith('chapter_') and f.endswith('.html'):
        p = os.path.join(d, f)
        with open(p, 'r', encoding='utf-8') as file:
            content = file.read()
            
        modified = False
        
        # Replace any {% extends "base.html" %} or {% extends 'base.html' %} with layout.html
        if 'extends "base.html"' in content or "extends 'base.html'" in content:
            content = content.replace('extends "base.html"', 'extends "layout.html"')
            content = content.replace("extends 'base.html'", 'extends "layout.html"')
            modified = True
            
        if modified:
            with open(p, 'w', encoding='utf-8') as file:
                file.write(content)

print("Updated all base.html extends links.")
