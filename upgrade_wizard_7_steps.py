import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update Constants to Lets
old_vars = """  const EXPECTED_ACCOUNTS = {{ property.expected_bills|default(8) }};
  const EXPECTED_WATER = {{ property.expected_water_meters|default(0) }};
  const EXPECTED_ELEC = {{ property.expected_elec_meters|default(0) }};
  const IS_BULK_WATER = {{ 'true' if property.is_bulk_water else 'false' }};
  const IS_BULK_ELEC = {{ 'true' if property.is_bulk_elec else 'false' }};
  const EXPECTED_BULK_WATER = IS_BULK_WATER && EXPECTED_WATER > 0 ? 1 : 0;
  const EXPECTED_SUB_WATER = EXPECTED_WATER - EXPECTED_BULK_WATER;
  
  const EXPECTED_BULK_ELEC = IS_BULK_ELEC && EXPECTED_ELEC > 0 ? 1 : 0;
  const EXPECTED_SUB_ELEC = EXPECTED_ELEC - EXPECTED_BULK_ELEC;"""

new_vars = """  let EXPECTED_ACCOUNTS = {{ property.expected_bills|default(8) }};
  let EXPECTED_WATER = {{ property.expected_water_meters|default(0) }};
  let EXPECTED_ELEC = {{ property.expected_elec_meters|default(0) }};
  let IS_BULK_WATER = {{ 'true' if property.is_bulk_water else 'false' }};
  let IS_BULK_ELEC = {{ 'true' if property.is_bulk_elec else 'false' }};
  
  let EXPECTED_BULK_WATER = IS_BULK_WATER && EXPECTED_WATER > 0 ? 1 : 0;
  let EXPECTED_SUB_WATER = Math.max(0, EXPECTED_WATER - EXPECTED_BULK_WATER);
  
  let EXPECTED_BULK_ELEC = IS_BULK_ELEC && EXPECTED_ELEC > 0 ? 1 : 0;
  let EXPECTED_SUB_ELEC = Math.max(0, EXPECTED_ELEC - EXPECTED_BULK_ELEC);"""
content = content.replace(old_vars, new_vars)

# 2. Update TOTAL_STEPS
content = content.replace("const TOTAL_STEPS = 6;", "const TOTAL_STEPS = 7;")

# 3. Add propertyMap to wizardData
content = content.replace("mapping: []\n  };", "mapping: [],\n    propertyMap: {}\n  };")

# 4. Inject gatherPropertyMap and recalculateRows
new_js_logic = """
  function gatherPropertyMap() {
    EXPECTED_ACCOUNTS = parseInt(document.getElementById('pm_accounts').value) || 0;
    EXPECTED_WATER = parseInt(document.getElementById('pm_water').value) || 0;
    EXPECTED_ELEC = parseInt(document.getElementById('pm_elec').value) || 0;
    IS_BULK_WATER = document.getElementById('pm_bulk_water').value === 'yes';
    IS_BULK_ELEC = document.getElementById('pm_bulk_elec').value === 'yes';

    EXPECTED_BULK_WATER = IS_BULK_WATER && EXPECTED_WATER > 0 ? 1 : 0;
    EXPECTED_SUB_WATER = Math.max(0, EXPECTED_WATER - EXPECTED_BULK_WATER);
    EXPECTED_BULK_ELEC = IS_BULK_ELEC && EXPECTED_ELEC > 0 ? 1 : 0;
    EXPECTED_SUB_ELEC = Math.max(0, EXPECTED_ELEC - EXPECTED_BULK_ELEC);

    wizardData.propertyMap = {
        accounts: EXPECTED_ACCOUNTS,
        water: EXPECTED_WATER,
        elec: EXPECTED_ELEC,
        bulkWater: IS_BULK_WATER,
        bulkElec: IS_BULK_ELEC
    };

    recalculateRows();
    return true;
  }

  function adjustContainer(containerId, expectedCount, rowGenFunc) {
      const container = document.getElementById(containerId);
      if (!container) return;
      while (container.children.length < expectedCount) {
          rowGenFunc(containerId);
      }
      while (container.children.length > expectedCount) {
          container.lastChild.remove();
      }
  }

  function recalculateRows() {
      adjustContainer('accounts-container', EXPECTED_ACCOUNTS, () => addAccountRow());
      adjustContainer('bulk-water-container', EXPECTED_BULK_WATER, (cid) => addMeterRow(cid));
      adjustContainer('bulk-elec-container', EXPECTED_BULK_ELEC, (cid) => addMeterRow(cid));
      adjustContainer('sub-water-container', EXPECTED_SUB_WATER, (cid) => addMeterRow(cid));
      adjustContainer('sub-elec-container', EXPECTED_SUB_ELEC, (cid) => addMeterRow(cid));
  }
"""
content = content.replace("// Initialization", new_js_logic + "\n  // Initialization")

# 5. Update nextStep
old_nextstep = """  function nextStep() {
    if (currentStep === 1) { if (!gatherAccounts()) return; }
    if (currentStep === 2) { gatherMeters('bulk-water'); }
    if (currentStep === 3) { gatherMeters('bulk-elec'); }
    if (currentStep === 4) { gatherMeters('sub-water'); gatherMeters('sub-elec'); }
    if (currentStep === 5) { gatherExceptions(); }"""

new_nextstep = """  function nextStep() {
    if (currentStep === 1) { if (!gatherPropertyMap()) return; }
    if (currentStep === 2) { if (!gatherAccounts()) return; }
    if (currentStep === 3) { gatherMeters('bulk-water'); }
    if (currentStep === 4) { gatherMeters('bulk-elec'); }
    if (currentStep === 5) { gatherMeters('sub-water'); gatherMeters('sub-elec'); }
    if (currentStep === 6) { gatherExceptions(); }"""
content = content.replace(old_nextstep, new_nextstep)


# 6. Shift HTML Steps
content = content.replace('id="step-6"', 'id="step-7"')
content = content.replace('<!-- STEP 6:', '<!-- STEP 7:')
content = content.replace('Step 6:', 'Step 7:')

content = content.replace('id="step-5"', 'id="step-6"')
content = content.replace('<!-- STEP 5:', '<!-- STEP 6:')
content = content.replace('Step 5:', 'Step 6:')

content = content.replace('id="step-4"', 'id="step-5"')
content = content.replace('<!-- STEP 4:', '<!-- STEP 5:')
content = content.replace('Step 4:', 'Step 5:')

content = content.replace('id="step-3"', 'id="step-4"')
content = content.replace('<!-- STEP 3:', '<!-- STEP 4:')
content = content.replace('Step 3:', 'Step 4:')

content = content.replace('id="step-2"', 'id="step-3"')
content = content.replace('<!-- STEP 2:', '<!-- STEP 3:')
content = content.replace('Step 2:', 'Step 3:')

content = content.replace('id="step-1"', 'id="step-2"')
content = content.replace('<!-- STEP 1:', '<!-- STEP 2:')
content = content.replace('Step 1:', 'Step 2:')

# 7. Inject New Step 1 (Property Setup)
new_step1 = """<!-- STEP 1: PROPERTY MAP -->
      <div id="step-1" class="wizard-step">
        <h2 class="text-xl font-bold text-slate-800 mb-2">Step 1: Property Setup Map</h2>
        <p class="text-sm text-slate-600 mb-6">Review and adjust the architectural totals for this property. Any changes here will instantly update the locked rows in subsequent steps.</p>
        
        <div class="grid grid-cols-2 gap-6 bg-slate-50 p-6 rounded-xl border border-slate-200 max-w-2xl mx-auto">
            <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Total Billable Accounts</label>
                <input type="number" id="pm_accounts" value="{{ property.expected_bills|default(8) }}" min="1" class="w-full border-2 border-slate-300 rounded px-3 py-2 outline-none focus:border-blue-500">
            </div>
            <div></div> <!-- Spacer -->
            
            <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Total Water Meters</label>
                <input type="number" id="pm_water" value="{{ property.expected_water_meters|default(0) }}" min="0" class="w-full border-2 border-slate-300 rounded px-3 py-2 outline-none focus:border-blue-500">
            </div>
            <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Total Electrical Meters</label>
                <input type="number" id="pm_elec" value="{{ property.expected_elec_meters|default(0) }}" min="0" class="w-full border-2 border-slate-300 rounded px-3 py-2 outline-none focus:border-blue-500">
            </div>
            
            <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Master Bulk Water?</label>
                <select id="pm_bulk_water" class="w-full border-2 border-slate-300 rounded px-3 py-2 outline-none focus:border-blue-500">
                    <option value="no" {% if not property.is_bulk_water %}selected{% endif %}>No</option>
                    <option value="yes" {% if property.is_bulk_water %}selected{% endif %}>Yes</option>
                </select>
            </div>
            <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Master Bulk Electrical?</label>
                <select id="pm_bulk_elec" class="w-full border-2 border-slate-300 rounded px-3 py-2 outline-none focus:border-blue-500">
                    <option value="no" {% if not property.is_bulk_elec %}selected{% endif %}>No</option>
                    <option value="yes" {% if property.is_bulk_elec %}selected{% endif %}>Yes</option>
                </select>
            </div>
        </div>
      </div>

      """

# Find insertion point (just before Step 2)
s2_idx = content.find('<!-- STEP 2:')
content = content[:s2_idx] + new_step1 + content[s2_idx:]

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Wizard upgraded to 7 steps with dynamic property map!")
