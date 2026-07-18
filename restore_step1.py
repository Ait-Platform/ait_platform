import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

restore_logic = """        wizardData = saved;
        
        // Restore Property Map Inputs
        if (wizardData.propertyMap && Object.keys(wizardData.propertyMap).length > 0) {
            document.getElementById('pm_accounts').value = wizardData.propertyMap.accounts || 1;
            document.getElementById('pm_water').value = wizardData.propertyMap.water || 0;
            document.getElementById('pm_elec').value = wizardData.propertyMap.elec || 0;
            document.getElementById('pm_owners').value = wizardData.propertyMap.owners || 1;
            document.getElementById('pm_addresses').value = wizardData.propertyMap.addresses || 1;
            document.getElementById('pm_bulk_water').value = wizardData.propertyMap.bulkWater ? 'yes' : 'no';
            document.getElementById('pm_bulk_elec').value = wizardData.propertyMap.bulkElec ? 'yes' : 'no';
            
            // Recalculate globals based on restored inputs
            EXPECTED_ACCOUNTS = wizardData.propertyMap.accounts || 1;
            EXPECTED_WATER = wizardData.propertyMap.water || 0;
            EXPECTED_ELEC = wizardData.propertyMap.elec || 0;
            EXPECTED_OWNERS = wizardData.propertyMap.owners || 1;
            EXPECTED_ADDRESSES = wizardData.propertyMap.addresses || 1;
            IS_BULK_WATER = wizardData.propertyMap.bulkWater || false;
            IS_BULK_ELEC = wizardData.propertyMap.bulkElec || false;
            
            EXPECTED_BULK_WATER = IS_BULK_WATER && EXPECTED_WATER > 0 ? 1 : 0;
            EXPECTED_SUB_WATER = Math.max(0, EXPECTED_WATER - EXPECTED_BULK_WATER);
            EXPECTED_BULK_ELEC = IS_BULK_ELEC && EXPECTED_ELEC > 0 ? 1 : 0;
            EXPECTED_SUB_ELEC = Math.max(0, EXPECTED_ELEC - EXPECTED_BULK_ELEC);
            
            if(typeof updateBulkVisibility === 'function') updateBulkVisibility();
        }"""

if "// Restore Property Map Inputs" not in html:
    html = html.replace("        wizardData = saved;", restore_logic)
    
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print("Injected propertyMap restoration logic into loadDraft().")
else:
    print("Logic already present.")
