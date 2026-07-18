with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('<!-- STEP 6: MAPPING -->', '<!-- STEP 7: MAPPING -->')
content = content.replace('Step 6: The Mapping Dashboard', 'Step 7: The Mapping Dashboard')

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Fixed step 7 text.")
