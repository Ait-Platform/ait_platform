with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

for i in range(1, 8):
    idx = content.find(f'id="step-{i}"')
    if idx != -1:
        print(content[idx-50:idx+150])
