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

# 2. Update the redirect in register_confirm
old_redirect = '''        elif subject == "cptd" or "sace" in subject:
            return redirect(url_for("sace_bp.catalog"))'''
new_redirect = '''        elif subject == "cptd" or "sace" in subject:
            next_url_val = session.get("reg_ctx", {}).get("next_url")
            if next_url_val and "/sace/provisioning" in next_url_val:
                return redirect(next_url_val)
            return redirect(url_for("sace_bp.catalog"))'''

text = text.replace(old_redirect, new_redirect)

# Wait, what if old_redirect is actually elif subject == "cptd": return redirect(url_for("sace_bp.catalog"))\n        elif subject.startswith("sace_"):
# Let me look up what it actually is in my previous grep.
