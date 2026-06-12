import os

d = r'D:\Users\yeshk\Documents\ait_platform\templates\school_home'
for f in os.listdir(d):
    if f.startswith('chapter_') and f.endswith('.html'):
        p = os.path.join(d, f)
        with open(p, 'r', encoding='utf-8') as file:
            content = file.read()
            
        # Check if form exists but lacks CSRF
        if 'method="POST"' in content and 'csrf_token' not in content:
            # We insert the CSRF token right after the form opening tag
            import re
            content = re.sub(
                r'(<form[^>]*method="POST"[^>]*>)',
                r'\1\n      <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>',
                content
            )
            
            with open(p, 'w', encoding='utf-8') as file:
                file.write(content)

print("Added CSRF tokens to all chapter forms.")
