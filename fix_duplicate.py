with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
skip = False
for i, line in enumerate(lines):
    if '@billing_bp.route("/billing/delete_property/<int:property_id>"' in line and i > 2000:
        skip = True
    
    if skip and line.strip() == "return redirect(url_for('billing_bp.learner_dashboard'))":
        skip = False
        continue
    
    if not skip:
        new_lines.append(line)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print('Duplicate delete_property removed.')
