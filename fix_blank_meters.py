import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Update gatherMeters
old_gather = """  function gatherMeters(containerId) {
    const container = document.getElementById(containerId + '-container');
    const rows = container.querySelectorAll('.meter-row');
    const list = [];
    rows.forEach((row, i) => {
      const num = row.querySelector('.meter-num').value.trim();
      if (num) list.push({ id: `${containerId}_${i}`, number: num });
    });
    
    if(containerId === 'bulk-water') wizardData.bulkWater = list;
    if(containerId === 'bulk-elec') wizardData.bulkElec = list;
    if(containerId === 'sub-water') wizardData.subWater = list;
    if(containerId === 'sub-elec') wizardData.subElec = list;
  }"""

new_gather = """  function gatherMeters(containerId, silent=false) {
    const container = document.getElementById(containerId + '-container');
    if (!container) return true;
    
    const rows = container.querySelectorAll('.meter-row');
    const list = [];
    let hasBlank = false;
    
    rows.forEach((row, i) => {
      const num = row.querySelector('.meter-num').value.trim();
      if (!num) hasBlank = true;
      list.push({ id: `${containerId}_${i}`, number: num });
    });
    
    if (!silent && hasBlank && rows.length > 0) {
      alert("Please fill in all meter numbers. You cannot leave a meter slot blank.");
      return false;
    }
    
    if(containerId === 'bulk-water') wizardData.bulkWater = list;
    if(containerId === 'bulk-elec') wizardData.bulkElec = list;
    if(containerId === 'sub-water') wizardData.subWater = list;
    if(containerId === 'sub-elec') wizardData.subElec = list;
    
    return true;
  }"""

content = content.replace(old_gather, new_gather)

# Update nextStep
old_nextstep_logic = """    if (currentStep === 1) { if (!gatherPropertyMap()) return; }
    if (currentStep === 2) { if (!gatherAccounts()) return; }
    if (currentStep === 3) { gatherMeters('bulk-water'); }
    if (currentStep === 4) { gatherMeters('bulk-elec'); }
    if (currentStep === 5) { gatherMeters('sub-water'); gatherMeters('sub-elec'); }
    
    // Global Meter Validation (Only block when finishing all meter steps)"""

new_nextstep_logic = """    if (currentStep === 1) { if (!gatherPropertyMap()) return; }
    if (currentStep === 2) { if (!gatherAccounts()) return; }
    if (currentStep === 3) { if(!gatherMeters('bulk-water')) return; }
    if (currentStep === 4) { if(!gatherMeters('bulk-elec')) return; }
    if (currentStep === 5) { 
        if(!gatherMeters('sub-water')) return; 
        if(!gatherMeters('sub-elec')) return; 
    }
    
    // Global Meter Validation (Only block when finishing all meter steps)"""

content = content.replace(old_nextstep_logic, new_nextstep_logic)

# Update auto save calls inside triggerAutoSave
old_autosave = """      gatherMeters('bulk-water');
      gatherMeters('bulk-elec');
      gatherMeters('sub-water');
      gatherMeters('sub-elec');"""

new_autosave = """      gatherMeters('bulk-water', true);
      gatherMeters('bulk-elec', true);
      gatherMeters('sub-water', true);
      gatherMeters('sub-elec', true);"""

content = content.replace(old_autosave, new_autosave)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Added validation for blank meter slots.")
