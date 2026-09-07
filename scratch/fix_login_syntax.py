import re

routes_path = 'app/auth/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

bad_chunk = '''    # Dynamically check if the user is an admin for any SACE subject
    is_sace_admin = any(s.startswith('sace') for s in session.get("admin_subjects", []))
    if is_sace_admin:
        return redirect(url_for("sace_bp.dashboard"))

        if not target:
            return False
        ref = urlparse(request.host_url)
        test = urlparse(urljoin(request.host_url, target))
    if not next_url:
        return redirect(url_for("auth_bp.bridge_dashboard"))'''

good_chunk = '''    # Dynamically check if the user is an admin for any SACE subject
    is_sace_admin = any(s.startswith('sace') for s in session.get("admin_subjects", []))
    if is_sace_admin:
        return redirect(url_for("sace_bp.dashboard"))

    if not next_url:
        return redirect(url_for("auth_bp.bridge_dashboard"))
    
    return redirect(next_url)'''

text = text.replace(bad_chunk, good_chunk)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
