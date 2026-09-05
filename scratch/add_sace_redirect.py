import re

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_code = """    # ---------- redirect ----------
    from urllib.parse import urljoin, urlparse
    def _is_safe_url(target: str) -> bool:"""

new_code = """    # ---------- redirect ----------
    
    # SACE Pre-Registered Personnel / Evaluator Override
    if email == 'nan@gmail.com' or 'sace' in admin_subjects:
        return redirect(url_for("sace_bp.dashboard"))
        
    from urllib.parse import urljoin, urlparse
    def _is_safe_url(target: str) -> bool:"""

text = text.replace(old_code, new_code)

with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
print("Added SACE pre-registered login redirect")
