import os
d = r'D:\Users\yeshk\Documents\ait_platform\templates\school_home'
for f in os.listdir(d):
    if f.startswith('chapter_') and f.endswith('.html'):
        p = os.path.join(d, f)
        with open(p, 'r', encoding='utf-8') as file:
            content = file.read()
        if 'extends "shared/base.html"' in content or "extends 'shared/base.html'" in content:
            content = content.replace('extends "shared/base.html"', 'extends "layout.html"')
            content = content.replace("extends 'shared/base.html'", 'extends "layout.html"')
            with open(p, 'w', encoding='utf-8') as file:
                file.write(content)
print("Updated all extends links.")
