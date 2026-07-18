import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Add IS_BULK
old_consts = """  const EXPECTED_ACCOUNTS = {{ property.expected_bills|default(8) }};
  const EXPECTED_WATER = {{ property.expected_water_meters|default(0) }};
  const EXPECTED_ELEC = {{ property.expected_elec_meters|default(0) }};"""
new_consts = """  const EXPECTED_ACCOUNTS = {{ property.expected_bills|default(8) }};
  const EXPECTED_WATER = {{ property.expected_water_meters|default(0) }};
  const EXPECTED_ELEC = {{ property.expected_elec_meters|default(0) }};
  const IS_BULK = {{ 'true' if property.is_bulk_metered else 'false' }};
  const EXPECTED_BULK_WATER = IS_BULK && EXPECTED_WATER > 0 ? 1 : 0;
  const EXPECTED_SUB_WATER = EXPECTED_WATER - EXPECTED_BULK_WATER;
  const EXPECTED_SUB_ELEC = EXPECTED_ELEC;"""
content = content.replace(old_consts, new_consts)

# 2. Modify Step 2 HTML to remove electric and buttons
old_step2 = """      <div id="step-2" class="wizard-step hidden">
        <div class="flex justify-between items-center mb-2">
            <h2 class="text-xl font-bold text-slate-800">Step 2: Bulk Meters Registry</h2>
            <div class="flex space-x-3 text-sm font-bold">
                <span class="px-3 py-1 bg-sky-100 text-sky-800 rounded-full" id="s2-water-count">Water: 0 / {{ property.expected_water_meters|default(0) }}</span>
                <span class="px-3 py-1 bg-indigo-100 text-indigo-800 rounded-full" id="s2-elec-count">Elec: 0 / {{ property.expected_elec_meters|default(0) }}</span>
            </div>
        </div>
        <p class="text-sm text-slate-500 mb-6">Enter the master bulk meter numbers for the entire property.</p>
        
        <div class="grid grid-cols-2 gap-8">
          <div>
            <h3 class="font-bold text-sky-700 mb-2 border-b border-sky-200 pb-1 flex justify-between">
              Bulk Water Meters
              <button onclick="addMeterRow('bulk-water')" class="text-xs bg-sky-100 text-sky-600 px-2 py-1 rounded hover:bg-sky-200">+ Add</button>
            </h3>
            <div id="bulk-water-container" class="space-y-2"></div>
          </div>
          <div>
            <h3 class="font-bold text-indigo-700 mb-2 border-b border-indigo-200 pb-1 flex justify-between">
              Bulk Electrical Meters
              <button onclick="addMeterRow('bulk-elec')" class="text-xs bg-indigo-100 text-indigo-600 px-2 py-1 rounded hover:bg-indigo-200">+ Add</button>
            </h3>
            <div id="bulk-elec-container" class="space-y-2"></div>
          </div>
        </div>
        <div class="mt-8 flex justify-between">"""

new_step2 = """      <div id="step-2" class="wizard-step hidden">
        <div class="flex justify-between items-center mb-2">
            <h2 class="text-xl font-bold text-slate-800">Step 2: Bulk Meter Registry</h2>
        </div>
        <p class="text-sm text-slate-500 mb-6">Enter the master bulk meter number for the entire property. Electrical uses standard tariffs and does not require a bulk meter.</p>
        
        <div class="max-w-md mx-auto">
          <div>
            <h3 class="font-bold text-sky-700 mb-2 border-b border-sky-200 pb-1">Bulk Water Meter</h3>
            <div id="bulk-water-container" class="space-y-2"></div>
          </div>
        </div>
        <div class="mt-8 flex justify-between">"""
content = content.replace(old_step2, new_step2)

# 3. Modify Step 3 HTML to remove buttons and counters
old_step3 = """      <div id="step-3" class="wizard-step hidden">
        <div class="flex justify-between items-center mb-2">
            <h2 class="text-xl font-bold text-slate-800">Step 3: Sub-Meters Registry</h2>
            <div class="flex space-x-3 text-sm font-bold">
                <span class="px-3 py-1 bg-sky-100 text-sky-800 rounded-full" id="s3-water-count">Water: 0 / {{ property.expected_water_meters|default(0) }}</span>
                <span class="px-3 py-1 bg-indigo-100 text-indigo-800 rounded-full" id="s3-elec-count">Elec: 0 / {{ property.expected_elec_meters|default(0) }}</span>
            </div>
        </div>
        <p class="text-sm text-slate-500 mb-6">Enter the sub-meters that will be linked to the individual accounts.</p>

        <div class="grid grid-cols-2 gap-8">
          <div>
            <h3 class="font-bold text-sky-700 mb-2 border-b border-sky-200 pb-1 flex justify-between">
              Sub Water Meters
              <button onclick="addMeterRow('sub-water')" class="text-xs bg-sky-100 text-sky-600 px-2 py-1 rounded hover:bg-sky-200">+ Add</button>
            </h3>
            <div id="sub-water-container" class="space-y-2"></div>
          </div>
          <div>
            <h3 class="font-bold text-indigo-700 mb-2 border-b border-indigo-200 pb-1 flex justify-between">
              Sub Electrical Meters
              <button onclick="addMeterRow('sub-elec')" class="text-xs bg-indigo-100 text-indigo-600 px-2 py-1 rounded hover:bg-indigo-200">+ Add</button>
            </h3>
            <div id="sub-elec-container" class="space-y-2"></div>
          </div>
        </div>
        <div class="mt-8 flex justify-between">"""

new_step3 = """      <div id="step-3" class="wizard-step hidden">
        <div class="flex justify-between items-center mb-2">
            <h2 class="text-xl font-bold text-slate-800">Step 3: Sub-Meters Registry</h2>
        </div>
        <p class="text-sm text-slate-500 mb-6">Enter the sub-meters that will be linked to the individual accounts. These rows are locked based on your Property Setup.</p>

        <div class="grid grid-cols-2 gap-8">
          <div>
            <h3 class="font-bold text-sky-700 mb-2 border-b border-sky-200 pb-1">Sub Water Meters</h3>
            <div id="sub-water-container" class="space-y-2"></div>
          </div>
          <div>
            <h3 class="font-bold text-indigo-700 mb-2 border-b border-indigo-200 pb-1">Sub Electrical Meters</h3>
            <div id="sub-elec-container" class="space-y-2"></div>
          </div>
        </div>
        <div class="mt-8 flex justify-between">"""
content = content.replace(old_step3, new_step3)

# 4. Modify addMeterRow
old_addmeter = """  function addMeterRow(containerId, val="") {
    const container = document.getElementById(containerId + '-container');
    const row = document.createElement('div');
    row.className = "flex items-center space-x-3 bg-white p-2 rounded-lg border border-slate-200 shadow-sm meter-row";
    row.innerHTML = `
      <div class="flex-grow">
        <input type="text" class="meter-num w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Meter Number" value="${val}">
      </div>
      <button type="button" onclick="this.closest('.meter-row').remove(); triggerAutoSave();" class="text-slate-400 hover:text-red-500 px-2"><svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg></button>
    `;
    container.appendChild(row);
  }"""
new_addmeter = """  function addMeterRow(containerId, val="") {
    const container = document.getElementById(containerId + '-container');
    const row = document.createElement('div');
    row.className = "flex items-center space-x-3 bg-white p-2 rounded-lg border border-slate-200 shadow-sm meter-row";
    row.innerHTML = `
      <div class="flex-grow">
        <input type="text" class="meter-num w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Meter Number" value="${val}">
      </div>
    `;
    container.appendChild(row);
  }"""
content = content.replace(old_addmeter, new_addmeter)

# 5. Modify loadDraft and initializeBlank
old_loaddraft = """  function loadDraft() {
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

        // Restore Meters
        if(wizardData.bulkWater) wizardData.bulkWater.forEach(m => addMeterRow('bulk-water', m.number));
        if(wizardData.bulkElec) wizardData.bulkElec.forEach(m => addMeterRow('bulk-elec', m.number));
        if(wizardData.subWater) wizardData.subWater.forEach(m => addMeterRow('sub-water', m.number));
        if(wizardData.subElec) wizardData.subElec.forEach(m => addMeterRow('sub-elec', m.number));
        
        if(document.getElementById('bulk-water-container').children.length === 0) addMeterRow('bulk-water');
        if(document.getElementById('sub-water-container').children.length === 0) addMeterRow('sub-water');
        if(document.getElementById('sub-elec-container').children.length === 0) addMeterRow('sub-elec');

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

      addMeterRow('bulk-water');
      addMeterRow('bulk-elec');
      addMeterRow('sub-water');
      addMeterRow('sub-elec');
      
      addExceptionRow();
  }"""

new_loaddraft = """  function loadDraft() {
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
      for(let i=0; i<EXPECTED_SUB_WATER; i++) addMeterRow('sub-water');
      for(let i=0; i<EXPECTED_SUB_ELEC; i++) addMeterRow('sub-elec');
      
      addExceptionRow();
  }"""
content = content.replace(old_loaddraft, new_loaddraft)

# 6. Remove Validation blocks from nextStep (since it's completely locked now)
old_nextstep = """  function nextStep() {
    if (currentStep === 1) { if (!gatherAccounts()) return; }
    if (currentStep === 2) { gatherMeters('bulk-water'); gatherMeters('bulk-elec'); }
    if (currentStep === 3) { 
        gatherMeters('sub-water'); gatherMeters('sub-elec'); 
        const wTotal = wizardData.bulkWater.length + wizardData.subWater.length;
        const eTotal = wizardData.bulkElec.length + wizardData.subElec.length;
        if (EXPECTED_WATER > 0 && wTotal !== EXPECTED_WATER) {
            alert(`Validation Failed: You have assigned ${wTotal} Water Meters, but the Property Map strictly expects exactly ${EXPECTED_WATER}. Please add or remove water meters across Step 2 and Step 3.`);
            return;
        }
        if (EXPECTED_ELEC > 0 && eTotal !== EXPECTED_ELEC) {
            alert(`Validation Failed: You have assigned ${eTotal} Electric Meters, but the Property Map strictly expects exactly ${EXPECTED_ELEC}. Please add or remove electric meters across Step 2 and Step 3.`);
            return;
        }
    }
    if (currentStep === 4) { gatherExceptions(); }"""
new_nextstep = """  function nextStep() {
    if (currentStep === 1) { if (!gatherAccounts()) return; }
    if (currentStep === 2) { gatherMeters('bulk-water'); gatherMeters('bulk-elec'); }
    if (currentStep === 3) { gatherMeters('sub-water'); gatherMeters('sub-elec'); }
    if (currentStep === 4) { gatherExceptions(); }"""
content = content.replace(old_nextstep, new_nextstep)

# 7. Remove updateLiveCounters logic
old_livecounters = """  function updateLiveCounters() {
      gatherMeters('bulk-water'); gatherMeters('bulk-elec');
      gatherMeters('sub-water'); gatherMeters('sub-elec');
      const wTotal = wizardData.bulkWater.length + wizardData.subWater.length;
      const eTotal = wizardData.bulkElec.length + wizardData.subElec.length;
      
      const wStr = `Water: ${wTotal} / ${EXPECTED_WATER}`;
      const eStr = `Elec: ${eTotal} / ${EXPECTED_ELEC}`;
      
      const el1 = document.getElementById('s2-water-count'); if(el1) el1.innerText = wStr;
      const el2 = document.getElementById('s3-water-count'); if(el2) el2.innerText = wStr;
      const el3 = document.getElementById('s2-elec-count'); if(el3) el3.innerText = eStr;
      const el4 = document.getElementById('s3-elec-count'); if(el4) el4.innerText = eStr;
      
      // Color code
      if(el1) el1.className = wTotal === EXPECTED_WATER ? "px-3 py-1 bg-emerald-100 text-emerald-800 rounded-full" : "px-3 py-1 bg-sky-100 text-sky-800 rounded-full";
      if(el2) el2.className = wTotal === EXPECTED_WATER ? "px-3 py-1 bg-emerald-100 text-emerald-800 rounded-full" : "px-3 py-1 bg-sky-100 text-sky-800 rounded-full";
      if(el3) el3.className = eTotal === EXPECTED_ELEC ? "px-3 py-1 bg-emerald-100 text-emerald-800 rounded-full" : "px-3 py-1 bg-indigo-100 text-indigo-800 rounded-full";
      if(el4) el4.className = eTotal === EXPECTED_ELEC ? "px-3 py-1 bg-emerald-100 text-emerald-800 rounded-full" : "px-3 py-1 bg-indigo-100 text-indigo-800 rounded-full";
  }

  function triggerAutoSave() {
    updateLiveCounters();
    if(saveTimeout) clearTimeout(saveTimeout);"""
new_livecounters = """  function triggerAutoSave() {
    if(saveTimeout) clearTimeout(saveTimeout);"""
content = content.replace(old_livecounters, new_livecounters)

old_load_live = """  document.addEventListener('DOMContentLoaded', () => {
    loadDraft();
    updateLiveCounters();
  });"""
new_load_live = """  document.addEventListener('DOMContentLoaded', () => {
    loadDraft();
  });"""
content = content.replace(old_load_live, new_load_live)


with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Setup Wizard strictly locked down.")
