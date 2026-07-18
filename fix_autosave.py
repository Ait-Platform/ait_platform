import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

old_trigger = """  function triggerAutoSave() {
    if(saveTimeout) clearTimeout(saveTimeout);
    saveTimeout = setTimeout(() => {
      // Gather current state from visible inputs without blocking
      gatherAccounts(true);
      gatherMeters('bulk-water', true);
      gatherMeters('bulk-elec', true);
      gatherMeters('sub-water', true);
      gatherMeters('sub-elec', true);
      gatherExceptions();
      gatherMapping();
      gatherInitialReadings(true);
      gatherArrears(true);
      gatherRates(true);
      gatherArrangements(true);
      gatherOwners();
      
      fetch("{{ url_for('billing_bp.save_architecture_draft', property_id=property.id) }}", {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': '{{ csrf_token() }}' },
        body: JSON.stringify(wizardData)
      }).then(r => {
        if(r.ok) {
          const ind = document.getElementById('autosave-indicator');
          ind.classList.remove('opacity-0');
          setTimeout(() => ind.classList.add('opacity-0'), 2000);
        }
      });
    }, 1000);
  }"""

new_trigger = """  let isSaving = false;
  let savePending = false;
  
  async function performAutoSave() {
    if (isSaving) {
        savePending = true;
        return;
    }
    isSaving = true;
    try {
        const r = await fetch("{{ url_for('billing_bp.save_architecture_draft', property_id=property.id) }}", {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-CSRFToken': '{{ csrf_token() }}' },
            body: JSON.stringify(wizardData)
        });
        if(r.ok) {
            const ind = document.getElementById('autosave-indicator');
            ind.classList.remove('opacity-0');
            setTimeout(() => ind.classList.add('opacity-0'), 2000);
        }
    } catch(e) {
        console.error(e);
    } finally {
        isSaving = false;
        if (savePending) {
            savePending = false;
            performAutoSave();
        }
    }
  }

  function triggerAutoSave() {
    if(saveTimeout) clearTimeout(saveTimeout);
    saveTimeout = setTimeout(() => {
      // Only gather from the active step to prevent wiping data from other steps!
      if (currentStep === 1) gatherPropertyMap(true);
      else if (currentStep === 2) gatherAccounts(true);
      else if (currentStep === 3) gatherMeters('bulk-water', true);
      else if (currentStep === 4) gatherMeters('bulk-elec', true);
      else if (currentStep === 5) { gatherMeters('sub-water', true); gatherMeters('sub-elec', true); }
      else if (currentStep === 6) gatherExceptions(true);
      else if (currentStep === 7) gatherMapping(true);
      else if (currentStep === 8) gatherInitialReadings(true);
      else if (currentStep === 9) gatherArrears(true);
      else if (currentStep === 10) gatherArrangements(true);
      else if (currentStep === 11) gatherRates(true);
      else if (currentStep === 12) gatherOwners(true);
      
      performAutoSave();
    }, 1000);
  }"""

html = html.replace(old_trigger, new_trigger)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(html)
print("Updated triggerAutoSave logic!")
