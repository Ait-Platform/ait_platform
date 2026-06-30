with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for line in lines:
    if '@billing_bp.route' in line and ('reading' in line or 'metsoa' in line):
        print(line.strip())
