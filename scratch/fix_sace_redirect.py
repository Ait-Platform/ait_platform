import re

routes_path = 'app/auth/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_logic = '''        elif subject == "sace":
            next_url_val = session.get("reg_ctx", {}).get("next_url") or "/"
            if "/sace/provisioning" in next_url_val or "/sace/claim_code" in next_url_val:
                return redirect(next_url_val)
            return redirect(url_for("sace_bp.dashboard"))'''

new_logic = '''        elif subject == "sace":
            if next_url and ("/sace/provisioning" in next_url or "/sace/claim_code" in next_url):
                return redirect(next_url)
            return redirect(url_for("sace_bp.dashboard"))'''

text = text.replace(old_logic, new_logic)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
