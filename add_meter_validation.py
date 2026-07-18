import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

validation_function = """
  function validateGlobalMeters() {
    const allMeters = [
        ...(wizardData.bulkWater || []),
        ...(wizardData.bulkElec || []),
        ...(wizardData.subWater || []),
        ...(wizardData.subElec || [])
    ];
    
    const seen = new Set();
    for (let m of allMeters) {
        if (!m.number) continue; // skip empty
        if (seen.has(m.number.toLowerCase())) {
            alert("Duplicate meter number found: " + m.number + ". Every meter must have a globally unique identifier!");
            return false;
        }
        seen.add(m.number.toLowerCase());
    }
    return true;
  }
"""

# Insert validation_function before nextStep
old_nextstep_start = "  function nextStep() {"
new_nextstep_start = validation_function + "\n  function nextStep() {"
content = content.replace(old_nextstep_start, new_nextstep_start)

# Inject call inside nextStep
old_nextstep_logic = """  function nextStep() {
    if (currentStep === 1) { if (!gatherPropertyMap()) return; }
    if (currentStep === 2) { if (!gatherAccounts()) return; }
    if (currentStep === 3) { gatherMeters('bulk-water'); }
    if (currentStep === 4) { gatherMeters('bulk-elec'); }
    if (currentStep === 5) { gatherMeters('sub-water'); gatherMeters('sub-elec'); }
    if (currentStep === 6) { gatherExceptions(); }

    triggerAutoSave();"""

new_nextstep_logic = """  function nextStep() {
    if (currentStep === 1) { if (!gatherPropertyMap()) return; }
    if (currentStep === 2) { if (!gatherAccounts()) return; }
    if (currentStep === 3) { gatherMeters('bulk-water'); }
    if (currentStep === 4) { gatherMeters('bulk-elec'); }
    if (currentStep === 5) { gatherMeters('sub-water'); gatherMeters('sub-elec'); }
    
    // Global Meter Validation
    if (currentStep >= 3 && currentStep <= 5) {
        if (!validateGlobalMeters()) return;
    }

    if (currentStep === 6) { gatherExceptions(); }

    triggerAutoSave();"""
content = content.replace(old_nextstep_logic, new_nextstep_logic)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Added validateGlobalMeters() logic.")
