import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('if(repId) updateExceptionDropdowns();', 'updateExceptionDropdowns();')

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Fixed updateExceptionDropdowns execution in addExceptionRow.")
