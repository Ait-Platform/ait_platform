import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_join = '''        code = (request.form.get("code") or "").strip().upper()
        if not code:
            flash("Please enter an access code.", "error")
            return redirect(url_for('sace_bp.auditor_join'))
            
        # Format if user forgot hyphen (assuming 8 chars)
        if len(code) == 8 and '-' not in code:
            code = f"{code[:4]}-{code[4:]}"'''

new_join = '''        code = (request.form.get("code") or "").strip().upper()
        if not code:
            flash("Please enter an access code.", "error")
            return redirect(url_for('sace_bp.auditor_join'))
            
        # Strip SACE- if they included it
        if code.startswith("SACE-"):
            code = code[5:].strip()
        if code.startswith("SACE "):
            code = code[5:].strip()
            
        # Format if user forgot hyphen (assuming 8 chars)
        code = code.replace(" ", "")
        if len(code) == 8 and '-' not in code:
            code = f"{code[:4]}-{code[4:]}"'''

text = text.replace(old_join, new_join)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
