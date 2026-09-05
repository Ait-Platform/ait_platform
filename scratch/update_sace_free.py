import re

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix free subject logic
old_free = 'if subject in ("cultural_fire", "culturalfire", "debtors", "mechanic", "cptd"):'
new_free = 'if subject in ("cultural_fire", "culturalfire", "debtors", "mechanic", "cptd", "sace", "sace_evaluator", "sace_facilitator", "sace_participant"):'
content = content.replace(old_free, new_free)

# Fix routing inside free logic
old_route = """        elif subject == "cptd":
            return redirect(url_for("sace_bp.selection_hub"))"""
new_route = """        elif subject == "cptd" or "sace" in subject:
            return redirect(url_for("sace_bp.selection_hub"))"""
content = content.replace(old_route, new_route)

with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated auth free subjects")
