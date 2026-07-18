import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_gather_mapping = """  function gatherMapping() {
    wizardData.mapping = [];
    document.querySelectorAll('.map-card').forEach(card => {"""

new_gather_mapping = """  function gatherMapping() {
    if (currentStep !== 7) return; // Protect mapping data from being wiped before dashboard is built
    
    wizardData.mapping = [];
    document.querySelectorAll('.map-card').forEach(card => {"""

if old_gather_mapping in content:
    content = content.replace(old_gather_mapping, new_gather_mapping)
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Fixed gatherMapping aggressively wiping data.")
else:
    print("Could not find gatherMapping.")
