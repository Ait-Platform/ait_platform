import re

filepath = 'app/program_uip/completion_routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search = '''    if request.method == "POST" and request.form.get("operation") == "reset_batch":
        session.pop('vault_batch_ref', None)
        session.pop('vault_source', None)
        session.pop('vault_date', None)
        return redirect(url_for("uip_bp.mo_vault_import" if is_mo_vault else "uip_bp.register_import", org_slug=org_slug))'''

replace = '''    if request.method == "POST" and request.form.get("operation") == "reset_batch":
        session.pop('vault_batch_ref', None)
        session.pop('vault_source', None)
        session.pop('vault_date', None)
        return redirect(url_for("uip_bp.mo_dashboard" if is_mo_vault else "uip_bp.secretary_workspace", org_slug=org_slug))'''

content = content.replace(search, replace)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated route to redirect to dashboard on end session")
