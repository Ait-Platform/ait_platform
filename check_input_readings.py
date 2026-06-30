with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if 'def input_readings(property_id):' in line:
        for j in range(i, i+30):
            if j < len(lines):
                print(f'{j}: {lines[j].strip()}')
        break
