with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    for line in f:
        if 'id="step-' in line and 'badge' not in line:
            print(line.strip())
