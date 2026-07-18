import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('<div id="step-2" class="wizard-step">', '<div id="step-2" class="wizard-step hidden">')

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Fixed hidden class on Step 2")
