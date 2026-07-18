with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    if 'db.session.rollback()' in line:
        new_lines.append('        try:\n            db.session.rollback()\n        except:\n            pass\n')
    elif 'return jsonify({"error": str(e)}), 500' in line:
        new_lines.append('        import traceback\n        return jsonify({"error": str(e) + " | " + traceback.format_exc()}), 500\n')
    else:
        new_lines.append(line)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
print('Updated routes.py rollback logic!')
