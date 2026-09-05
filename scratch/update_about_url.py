import re

with open('app/public/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_logic = """        if subj.about_endpoint:
            try:
                subj.about_url = url_for(subj.about_endpoint)
            except BuildError:
                subj.about_url = None"""

new_logic = """        if subj.about_endpoint:
            try:
                subj.about_url = url_for(subj.about_endpoint)
            except BuildError:
                subj.about_url = None
        else:
            subj.about_url = url_for('auth_bp.register', subject=subj.slug)"""

content = content.replace(old_logic, new_logic)

with open('app/public/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated public routes about_url fallback")
