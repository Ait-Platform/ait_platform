with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

bad_code = "        if is_sace_admin:\n        return redirect(url_for('sace_bp.dashboard'))"
good_code = "        if is_sace_admin:\n            return redirect(url_for('sace_bp.dashboard'))"

text = text.replace(bad_code, good_code)

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
print("Fixed indentation error")
