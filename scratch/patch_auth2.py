import re
with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Update _infer_subject_from_next
old_infer = '''        if "/spv" in n_url_lower or "/portfolio" in n_url_lower:
            return "spv"'''
new_infer = '''        if "/spv" in n_url_lower or "/portfolio" in n_url_lower:
            return "spv"
        if "/sace" in n_url_lower:
            return "sace"'''
text = text.replace(old_infer, new_infer)

# 2. Update the free subjects check
old_free = '''    if subject in ("cultural_fire", "culturalfire", "debtors", "mechanic", "cptd"):'''
new_free = '''    if subject in ("cultural_fire", "culturalfire", "debtors", "mechanic", "cptd", "sace"):'''
text = text.replace(old_free, new_free)

# 3. Update the redirect inside the free block
old_redirect = '''        elif subject == "cptd":
            return redirect(url_for("sace_bp.catalog"))
        elif subject.startswith("sace_"):'''
new_redirect = '''        elif subject == "cptd":
            return redirect(url_for("sace_bp.catalog"))
        elif subject == "sace":
            next_url_val = session.get("reg_ctx", {}).get("next_url") or "/"
            if "/sace/provisioning" in next_url_val:
                return redirect(next_url_val)
            return redirect(url_for("sace_bp.dashboard"))
        elif subject.startswith("sace_"):'''
text = text.replace(old_redirect, new_redirect)

with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
