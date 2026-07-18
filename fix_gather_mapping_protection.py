import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_gather_mapping = """  function gatherMapping() {
    if (currentStep !== 7) return; // Protect mapping data from being wiped before dashboard is built
    
    wizardData.mapping = [];"""

new_gather_mapping = """  function gatherMapping() {
    const container = document.getElementById('mapping-container');
    if (!container || container.children.length === 0) return; // Protect mapping data from being wiped before dashboard is built
    
    wizardData.mapping = [];"""

if old_gather_mapping in content:
    content = content.replace(old_gather_mapping, new_gather_mapping)
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Fixed gatherMapping protection logic.")
else:
    print("Could not find gatherMapping logic to replace.")
