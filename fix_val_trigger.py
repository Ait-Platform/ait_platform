import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_logic = """    // Global Meter Validation
    if (currentStep >= 3 && currentStep <= 5) {
        if (!validateGlobalMeters()) return;
    }"""

new_logic = """    // Global Meter Validation (Only block when finishing all meter steps)
    if (currentStep === 5) {
        if (!validateGlobalMeters()) return;
    }"""

if old_logic in content:
    content = content.replace(old_logic, new_logic)
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Fixed validation trigger.")
else:
    print("Could not find the old validation logic.")
