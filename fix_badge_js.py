import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_labels = "const labels = ['1. Accounts', '2. Bulk Meters', '3. Sub Meters', '4. Exceptions', '5. Mapping'];"
new_labels = "const labels = ['1. Property', '2. Accounts', '3. Bulk Water', '4. Bulk Elec', '5. Sub Meters', '6. Exceptions', '7. Mapping'];"
content = content.replace(old_labels, new_labels)

old_step4 = "if(step === 4) {"
new_step6 = "if(step === 6) {"
content = content.replace(old_step4, new_step6)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Fixed hardcoded badge labels and exception step trigger.")
