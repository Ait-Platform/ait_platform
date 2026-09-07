import re

routes_path = 'app/auth/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_login_logic = '''    # SACE Pre-Registered Personnel / Evaluator Override
    # Dynamically check if the user is an admin for any SACE subject
    is_sace_admin = any(s.startswith('sace') for s in session.get("admin_subjects", []))
    if is_sace_admin:
        return redirect(url_for("sace_bp.dashboard"))
        
    from urllib.parse import urljoin, urlparse
    def _is_safe_url(target: str) -> bool:'''

new_login_logic = '''    # SACE Pre-Registered Personnel / Evaluator Override
    from urllib.parse import urljoin, urlparse
    def _is_safe_url(target: str) -> bool:
        if not target:
            return False
        ref = urlparse(request.host_url)
        test = urlparse(urljoin(request.host_url, target))
        return (
            (test.scheme in ("http", "https"))
            and (ref.netloc == test.netloc)
        )

    next_url = request.args.get("next")
    
    # Intercept legacy Paystack_start redirects (from cached URLs) to use register_decision instead
    if next_url and "Paystack_start" in next_url and "spv" not in next_url:
        import re as regex
        m = regex.search(r'subject=([^&]+)', next_url)
        subj = m.group(1) if m else "cultural_fire"
        next_url = url_for("auth_bp.register_decision", subject=subj)

    if next_url and _is_safe_url(next_url):
        return redirect(next_url)

    # Dynamically check if the user is an admin for any SACE subject
    is_sace_admin = any(s.startswith('sace') for s in session.get("admin_subjects", []))
    if is_sace_admin:
        return redirect(url_for("sace_bp.dashboard"))
'''

# We also need to remove the duplicate _is_safe_url and next_url handling further down
text = text.replace(old_login_logic, new_login_logic)

# Clean up the duplicate chunk below it
duplicate_chunk = '''        return (
            (test.scheme in ("http", "https"))
            and (ref.netloc == test.netloc)
        )

    next_url = request.args.get("next")
    
    # Intercept legacy Paystack_start redirects (from cached URLs) to use register_decision instead
    if next_url and "Paystack_start" in next_url and "spv" not in next_url:
        import re
        m = re.search(r'subject=([^&]+)', next_url)
        subj = m.group(1) if m else "cultural_fire"
        next_url = url_for("auth_bp.register_decision", subject=subj)

    if not _is_safe_url(next_url):
        next_url = url_for("auth_bp.bridge_dashboard")
    return redirect(next_url)'''

text = text.replace(duplicate_chunk, '''    if not next_url:
        return redirect(url_for("auth_bp.bridge_dashboard"))''')

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
