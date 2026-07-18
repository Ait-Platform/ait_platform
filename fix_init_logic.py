import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

start_idx = content.find('  function loadDraft() {')
end_idx = content.find('  // --- STEP 1: ACCOUNTS ---')

new_js = """  function loadDraft() {
    const saved = {{ draft_json|safe }};
    if (saved && saved.accounts) {
      try {
        wizardData = saved;
        
        // Restore Accounts
        if(wizardData.accounts.length === 0) {
            for(let i=0; i<EXPECTED_ACCOUNTS; i++) addAccountRow();
        } else {
            wizardData.accounts.forEach(acc => {
                addAccountRow(acc.number, acc.owner, acc.isBulk);
            });
        }

        // Strict Meter Row Generation
        const bwList = wizardData.bulkWater || [];
        for(let i=0; i<EXPECTED_BULK_WATER; i++) {
            addMeterRow('bulk-water', i < bwList.length ? bwList[i].number : "");
        }
        
        const beList = wizardData.bulkElec || [];
        for(let i=0; i<EXPECTED_BULK_ELEC; i++) {
            addMeterRow('bulk-elec', i < beList.length ? beList[i].number : "");
        }
        
        const swList = wizardData.subWater || [];
        for(let i=0; i<EXPECTED_SUB_WATER; i++) {
            addMeterRow('sub-water', i < swList.length ? swList[i].number : "");
        }
        
        const seList = wizardData.subElec || [];
        for(let i=0; i<EXPECTED_SUB_ELEC; i++) {
            addMeterRow('sub-elec', i < seList.length ? seList[i].number : "");
        }

        // Restore Exceptions
        if(wizardData.exceptions) wizardData.exceptions.forEach(exc => addExceptionRow(exc.stolen_num, exc.replacement_id));

      } catch(e) {
        console.error("Draft load failed", e);
        initializeBlank();
      }
    } else {
      initializeBlank();
    }
  }

  function initializeBlank() {
      for(let i=0; i<EXPECTED_ACCOUNTS; i++) addAccountRow();
      const firstRadio = document.querySelector('input[name="bulk_idx"]');
      if (firstRadio) firstRadio.checked = true;

      for(let i=0; i<EXPECTED_BULK_WATER; i++) addMeterRow('bulk-water');
      for(let i=0; i<EXPECTED_BULK_ELEC; i++) addMeterRow('bulk-elec');
      for(let i=0; i<EXPECTED_SUB_WATER; i++) addMeterRow('sub-water');
      for(let i=0; i<EXPECTED_SUB_ELEC; i++) addMeterRow('sub-elec');
      
      addExceptionRow();
  }

"""

new_content = content[:start_idx] + new_js + content[end_idx:]

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(new_content)

print("loadDraft and initializeBlank successfully fixed!")
