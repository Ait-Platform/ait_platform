with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if 'flex flex-col' in line or 'Property Data Table' in line:
        print(str(i) + ': ' + line.strip())
