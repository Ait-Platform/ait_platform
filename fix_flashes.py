import os, glob

template_dir = r"D:\Users\yeshk\Documents\ait_platform\templates\program_budget"
for file_path in glob.glob(os.path.join(template_dir, "*.html")):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    modified = False
    
    if "{% block flashes %}" not in content and "{% extends" in content:
        content = content.replace('{% extends "layout.html" %}', '{% extends "layout.html" %}\n{% block flashes %}{% endblock %}')
        modified = True
        
    if "partials/flash_messages.html" not in content and '<div class="p-8">' in content:
        content = content.replace('<div class="p-8">', '<div class="p-8">\n      {% include "partials/flash_messages.html" %}')
        modified = True
        
    if modified:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Updated {file_path}")

print("Done injecting flashes")
