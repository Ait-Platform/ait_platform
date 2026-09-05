import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the broken check
old_code = """    # 1. If Evaluator/Admin, route them to the main Hub
    if current_user.is_admin_global():  # using is_admin_global() or similar? Let's just check roles
        pass # we will do a safe check
    
    # Safe admin check:"""

new_code = """    # 1. If Evaluator/Admin, route them to the main Hub
    # Safe admin check:"""

content = content.replace(old_code, new_code)

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Removed broken is_admin_global check")
