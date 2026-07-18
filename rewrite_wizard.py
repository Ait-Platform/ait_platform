import os

content = """{% extends "layout.html" %}

{% block title %}Global Architecture Setup{% endblock %}

{% block content %}
<div class="max-w-6xl mx-auto px-4 py-8">
  <div class="bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden min-h-[70vh] flex flex-col">
    <!-- Header -->
    <div class="bg-slate-50 px-8 py-5 border-b border-slate-200 flex justify-between items-center">
      <div>
        <h1 class="text-2xl font-extrabold text-slate-800">Architecture Mapping</h1>
        <p class="text-sm text-slate-500 mt-1">Property: <span class="font-bold text-slate-700">{{ property.name }}</span></p>
      </div>
      <div class="flex items-center space-x-6">
        <div class="flex space-x-2 text-[11px]">
          <span id="step-1-badge" class="px-2 py-1 rounded-full font-bold bg-blue-100 text-blue-700">1. Accounts</span>
          <span id="step-2-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">2. Bulk Meters</span>
          <span id="step-3-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">3. Sub Meters</span>
          <span id="step-4-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">4. Exceptions</span>
          <span id="step-5-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">5. Mapping</span>
        </div>
        <a href="{{ url_for('billing_bp.learner_dashboard') }}" class="text-sm font-bold text-slate-500 hover:text-slate-800 border border-slate-300 px-3 py-1.5 rounded bg-white shadow-sm transition">
          Back to Dashboard
        </a>
      </div>
    </div>

    <!-- Wizard Steps Container -->
    <div class="p-8 flex-grow">
      
      <!-- STEP 1: ACCOUNTS -->
      <div id="step-1" class="wizard-step">
        <h2 class="text-xl font-bold text-slate-800 mb-2">Step 1: Account Registry</h2>
        <p class="text-sm text-slate-600 mb-6">Enter all the Municipal Account Numbers for this property. Select exactly one account as the <strong>Bulk Account</strong>.</p>
        
        <div class="bg-slate-50 rounded-xl border border-slate-200 p-4 mb-4">
          <div class="grid grid-cols-12 gap-4 text-xs font-bold text-slate-500 uppercase tracking-wider mb-2 px-2">
            <div class="col-span-5">Account Number</div>
            <div class="col-span-5">Owner Name</div>
            <div class="col-span-2 text-center">Is Bulk?</div>
          </div>
          <div id="accounts-container" class="space-y-3">
            <!-- Populated by JS based on expected_bills -->
          </div>
        </div>
      </div>

      <!-- STEP 2: BULK METERS -->
      <div id="step-2" class="wizard-step hidden">
        <h2 class="text-xl font-bold text-slate-800 mb-2">Step 2: Bulk Meters Registry</h2>
        <p class="text-sm text-slate-600 mb-6">List ALL the master physical meters that belong to your Bulk Account.</p>
        
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div class="bg-sky-50 rounded-xl border border-sky-200 p-4">
            <h3 class="font-bold text-sky-800 mb-3 text-sm flex items-center"><span class="w-2 h-2 rounded-full bg-sky-500 mr-2"></span>Bulk Water Meters</h3>
            <div id="bulk-water-container" class="space-y-3"></div>
            <button type="button" onclick="addMeterRow('bulk-water')" class="mt-4 text-sm font-bold text-sky-700 hover:text-sky-900 flex items-center">
              + Add Bulk Water Meter
            </button>
          </div>
          <div class="bg-indigo-50 rounded-xl border border-indigo-200 p-4">
            <h3 class="font-bold text-indigo-800 mb-3 text-sm flex items-center"><span class="w-2 h-2 rounded-full bg-indigo-500 mr-2"></span>Bulk Electric Meters</h3>
            <div id="bulk-elec-container" class="space-y-3"></div>
            <button type="button" onclick="addMeterRow('bulk-elec')" class="mt-4 text-sm font-bold text-indigo-700 hover:text-indigo-900 flex items-center">
              + Add Bulk Electric Meter
            </button>
          </div>
        </div>
      </div>

      <!-- STEP 3: SUB METERS -->
      <div id="step-3" class="wizard-step hidden">
        <h2 class="text-xl font-bold text-slate-800 mb-2">Step 3: Sub-Meters Registry</h2>
        <p class="text-sm text-slate-600 mb-6">List ALL the normal tenant sub-meters on the property.</p>
        
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div class="bg-sky-50 rounded-xl border border-sky-200 p-4">
            <h3 class="font-bold text-sky-800 mb-3 text-sm flex items-center"><span class="w-2 h-2 rounded-full bg-sky-500 mr-2"></span>Sub Water Meters</h3>
            <div id="sub-water-container" class="space-y-3"></div>
            <button type="button" onclick="addMeterRow('sub-water')" class="mt-4 text-sm font-bold text-sky-700 hover:text-sky-900 flex items-center">
              + Add Sub Water Meter
            </button>
          </div>
          <div class="bg-indigo-50 rounded-xl border border-indigo-200 p-4">
            <h3 class="font-bold text-indigo-800 mb-3 text-sm flex items-center"><span class="w-2 h-2 rounded-full bg-indigo-500 mr-2"></span>Sub Electric Meters</h3>
            <div id="sub-elec-container" class="space-y-3"></div>
            <button type="button" onclick="addMeterRow('sub-elec')" class="mt-4 text-sm font-bold text-indigo-700 hover:text-indigo-900 flex items-center">
              + Add Sub Electric Meter
            </button>
          </div>
        </div>
      </div>

      <!-- STEP 4: EXCEPTIONAL CASES -->
      <div id="step-4" class="wizard-step hidden">
        <h2 class="text-xl font-bold text-slate-800 mb-2">Step 4: Exceptional Cases (Stolen Meters)</h2>
        <p class="text-sm text-slate-600 mb-6">List any Stolen Municipal Meters here, and select which new Sub-Meter physically replaced it.</p>
        
        <div class="bg-rose-50 rounded-xl border border-rose-200 p-4 max-w-3xl">
          <div id="exceptions-container" class="space-y-3"></div>
          <button type="button" onclick="addExceptionRow()" class="mt-4 text-sm font-bold text-rose-700 hover:text-rose-900 flex items-center">
            + Add Exception Link
          </button>
        </div>
      </div>

      <!-- STEP 5: MAPPING -->
      <div id="step-5" class="wizard-step hidden">
        <h2 class="text-xl font-bold text-slate-800 mb-2">Step 5: The Mapping Dashboard</h2>
        <p class="text-sm text-slate-600 mb-6">Because you defined your Bulk and Exception meters explicitly, the system already knows what to do with them! You only need to link your Sub-Meters to your Sub-Accounts.</p>
        
        <div id="mapping-container" class="space-y-6">
          <!-- Populated by JS -->
        </div>
      </div>

    </div>

    <!-- Footer Controls -->
    <div class="bg-slate-50 px-8 py-5 border-t border-slate-200 flex justify-between items-center">
      <div>
         <span id="autosave-indicator" class="text-xs text-emerald-600 font-bold opacity-0 transition-opacity">Draft auto-saved!</span>
      </div>
      <div class="flex space-x-3">
        <button type="button" id="btn-prev" onclick="prevStep()" class="hidden px-6 py-2 border border-slate-300 text-slate-600 font-bold rounded-lg hover:bg-slate-100 transition">Back</button>
        <button type="button" id="btn-next" onclick="nextStep()" class="px-8 py-2 bg-blue-600 text-white font-bold rounded-lg shadow-sm hover:bg-blue-700 transition">Next Step</button>
        <button type="button" id="btn-save" onclick="saveArchitecture()" class="hidden px-8 py-2 bg-emerald-600 text-white font-bold rounded-lg shadow-sm hover:bg-emerald-700 transition">Save &amp; Finalize Architecture</button>
      </div>
    </div>
  </div>
</div>

<script>
  let currentStep = 1;
  const TOTAL_STEPS = 5;
  const DRAFT_KEY = 'billing_wizard_draft_{{ property.id }}';
  const EXPECTED_ACCOUNTS = {{ property.expected_bills|default(8) }};

  // Data stores
  let wizardData = {
    accounts: [],
    bulkWater: [],
    bulkElec: [],
    subWater: [],
    subElec: [],
    exceptions: [],
    mapping: []
  };

  // Initialization
  document.addEventListener('DOMContentLoaded', () => {
    loadDraft();
    
    // Attach autosave listeners to all inputs
    document.getElementById('step-1').addEventListener('input', triggerAutoSave);
    document.getElementById('step-2').addEventListener('input', triggerAutoSave);
    document.getElementById('step-3').addEventListener('input', triggerAutoSave);
    document.getElementById('step-4').addEventListener('input', triggerAutoSave);
    document.getElementById('step-5').addEventListener('input', triggerAutoSave);
    
    // For radio buttons / selects
    document.getElementById('step-1').addEventListener('change', triggerAutoSave);
    document.getElementById('step-4').addEventListener('change', triggerAutoSave);
    document.getElementById('step-5').addEventListener('change', triggerAutoSave);
  });

  function updateBadges() {
    for(let i=1; i<=TOTAL_STEPS; i++) {
      const badge = document.getElementById(`step-${i}-badge`);
      if (i === currentStep) {
        badge.className = "px-2 py-1 rounded-full font-bold bg-blue-100 text-blue-700";
      } else if (i < currentStep) {
        badge.className = "px-2 py-1 rounded-full font-bold bg-emerald-100 text-emerald-700";
        badge.innerHTML = `✓ ${i}`;
      } else {
        badge.className = "px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400";
        // restore label
        const labels = ['1. Accounts', '2. Bulk Meters', '3. Sub Meters', '4. Exceptions', '5. Mapping'];
        badge.innerHTML = labels[i-1];
      }
    }
  }

  function showStep(step) {
    document.querySelectorAll('.wizard-step').forEach(el => el.classList.add('hidden'));
    document.getElementById(`step-${step}`).classList.remove('hidden');
    
    document.getElementById('btn-prev').classList.toggle('hidden', step === 1);
    
    if (step === TOTAL_STEPS) {
      document.getElementById('btn-next').classList.add('hidden');
      document.getElementById('btn-save').classList.remove('hidden');
      buildMappingDashboard();
    } else {
      document.getElementById('btn-next').classList.remove('hidden');
      document.getElementById('btn-save').classList.add('hidden');
    }

    if(step === 4) {
      updateExceptionDropdowns();
    }
    
    updateBadges();
  }

  function nextStep() {
    if (currentStep === 1) { if (!gatherAccounts()) return; }
    if (currentStep === 2) { gatherMeters('bulk-water'); gatherMeters('bulk-elec'); }
    if (currentStep === 3) { gatherMeters('sub-water'); gatherMeters('sub-elec'); }
    if (currentStep === 4) { gatherExceptions(); }

    triggerAutoSave();

    if (currentStep < TOTAL_STEPS) {
      currentStep++;
      showStep(currentStep);
    }
  }

  function prevStep() {
    triggerAutoSave();
    if (currentStep > 1) {
      currentStep--;
      showStep(currentStep);
    }
  }

  // --- AUTO-SAVE LOGIC ---
  let saveTimeout = null;
  function triggerAutoSave() {
    if(saveTimeout) clearTimeout(saveTimeout);
    saveTimeout = setTimeout(() => {
      // Gather current state from visible inputs without blocking
      gatherAccounts(true);
      gatherMeters('bulk-water');
      gatherMeters('bulk-elec');
      gatherMeters('sub-water');
      gatherMeters('sub-elec');
      gatherExceptions();
      gatherMapping();
      
      localStorage.setItem(DRAFT_KEY, JSON.stringify(wizardData));
      
      const ind = document.getElementById('autosave-indicator');
      ind.classList.remove('opacity-0');
      setTimeout(() => ind.classList.add('opacity-0'), 2000);
    }, 1000);
  }

  function loadDraft() {
    const saved = localStorage.getItem(DRAFT_KEY);
    if (saved) {
      try {
        wizardData = JSON.parse(saved);
        
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
      if(firstRadio) firstRadio.checked = true;
      addMeterRow('bulk-water');
      addMeterRow('sub-water');
      addMeterRow('sub-elec');
  }

  // --- STEP 1: ACCOUNTS ---
  function addAccountRow(accNum="", ownerName="", isBulk=false) {
    const container = document.getElementById('accounts-container');
    const idx = container.children.length;
    const row = document.createElement('div');
    row.className = "grid grid-cols-12 gap-4 items-center bg-white p-2 rounded-lg border border-slate-200 shadow-sm acc-row";
    row.innerHTML = `
      <div class="col-span-5">
        <input type="text" class="acc-num w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Account Number" value="${accNum}">
      </div>
      <div class="col-span-5">
        <input type="text" class="acc-owner w-full rounded border-slate-300 px-3 py-1.5 text-sm focus:border-blue-500 outline-none" placeholder="Owner Name" value="${ownerName}">
      </div>
      <div class="col-span-2 flex justify-center items-center">
        <input type="radio" name="bulk_idx" value="${idx}" class="w-4 h-4 text-blue-600 focus:ring-blue-500 cursor-pointer" ${isBulk ? 'checked' : ''}>
      </div>
    `;
    container.appendChild(row);
  }

  function gatherAccounts(silent=false) {
    wizardData.accounts = [];
    const rows = document.querySelectorAll('.acc-row');
    let hasBulk = false;
    
    rows.forEach((row, i) => {
      const accNum = row.querySelector('.acc-num').value.trim();
      const owner = row.querySelector('.acc-owner').value.trim();
      const isBulk = row.querySelector('input[name="bulk_idx"]').checked;
      if (isBulk) hasBulk = true;
      
      wizardData.accounts.push({ id: `acc_${i}`, number: accNum, owner: owner, isBulk: isBulk });
    });

    if (!silent) {
      if (wizardData.accounts.length === 0) { alert("Please enter accounts."); return false; }
      if (!hasBulk) { alert("Please select exactly one Bulk Account."); return false; }
    }
    return true;
  }

  // --- STEP 2 & 3: METERS ---
  function addMeterRow(containerId, val="") {
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
  }

  function gatherMeters(containerId) {
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
  }

  // --- STEP 4: EXCEPTIONS ---
  function addExceptionRow(stolenNum="", repId="") {
    const container = document.getElementById('exceptions-container');
    const row = document.createElement('div');
    row.className = "flex items-center space-x-4 bg-white p-3 rounded-lg border border-rose-200 shadow-sm exc-row";
    
    row.innerHTML = `
      <div class="flex-1">
        <label class="block text-[10px] font-bold text-rose-800 uppercase mb-1">Stolen Municipal Meter No.</label>
        <input type="text" class="exc-stolen-num w-full rounded border-rose-300 px-2 py-1.5 text-xs outline-none" placeholder="e.g. CEL884" value="${stolenNum}">
      </div>
      <div class="flex items-center pt-5 px-2">
        <svg class="w-5 h-5 text-rose-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14 5l7 7m0 0l-7 7m7-7H3"></path></svg>
      </div>
      <div class="flex-1">
        <label class="block text-[10px] font-bold text-emerald-800 uppercase mb-1">Replaced By (Sub-Meter)</label>
        <select class="exc-replacement-id w-full rounded border-emerald-300 px-2 py-1.5 text-xs outline-none bg-white" data-selected="${repId}">
          <!-- Options populated by updateExceptionDropdowns() -->
        </select>
      </div>
      <button type="button" onclick="this.closest('.exc-row').remove(); triggerAutoSave();" class="mt-5 text-slate-400 hover:text-red-500">
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
      </button>
    `;
    container.appendChild(row);
    if(repId) updateExceptionDropdowns();
  }

  function updateExceptionDropdowns() {
    let opts = '<option value="">Select Sub-Meter...</option>';
    wizardData.subWater.forEach(m => opts += `<option value="${m.id}">${m.number} (Water)</option>`);
    wizardData.subElec.forEach(m => opts += `<option value="${m.id}">${m.number} (Elec)</option>`);
    
    document.querySelectorAll('.exc-replacement-id').forEach(sel => {
      const val = sel.value || sel.dataset.selected;
      sel.innerHTML = opts;
      if (val) sel.value = val;
    });
  }

  function gatherExceptions() {
    wizardData.exceptions = [];
    document.querySelectorAll('.exc-row').forEach(row => {
      const stolenNum = row.querySelector('.exc-stolen-num').value.trim();
      const repId = row.querySelector('.exc-replacement-id').value;
      if(stolenNum && repId) {
        wizardData.exceptions.push({ stolen_num: stolenNum, replacement_id: repId });
      }
    });
  }

  // --- STEP 5: MAPPING ---
  function buildMappingDashboard() {
    const container = document.getElementById('mapping-container');
    container.innerHTML = ''; 

    let wOpts = '<option value="">-- No Water Meter --</option>';
    wizardData.subWater.forEach(m => wOpts += `<option value="${m.id}">${m.number}</option>`);

    let eOpts = '<option value="">-- No Elec Meter --</option>';
    wizardData.subElec.forEach(m => eOpts += `<option value="${m.id}">${m.number}</option>`);

    wizardData.accounts.forEach(acc => {
      if(!acc.number) return; // Skip empty rows

      const card = document.createElement('div');
      card.className = `p-5 rounded-xl border-2 ${acc.isBulk ? 'bg-amber-50 border-amber-300' : 'bg-white border-slate-200'} shadow-sm map-card`;
      card.dataset.accId = acc.id;

      if (acc.isBulk) {
        // Bulk mapping is automatic based on Steps 2!
        card.innerHTML = `
          <div class="flex justify-between items-center mb-2">
            <h3 class="font-bold text-lg text-slate-800">${acc.number} <span class="ml-2 px-2 py-0.5 bg-amber-200 text-amber-800 text-xs rounded-full uppercase">Bulk Account</span></h3>
          </div>
          <div class="bg-amber-100 p-3 rounded text-sm text-amber-800 font-medium">
             <svg class="w-4 h-4 inline-block mr-1 mb-0.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path></svg>
             Auto-Mapped! The ${wizardData.bulkWater.length} Bulk Water and ${wizardData.bulkElec.length} Bulk Electric meters you defined are automatically linked to this account.
          </div>
        `;
      } else {
        // Sub mapping
        // Check if we have saved mapping for this
        const savedMap = wizardData.mapping.find(x => x.account_id === acc.id) || { water: '', elec: '' };

        card.innerHTML = `
          <div class="flex justify-between items-center mb-4 pb-2 border-b border-slate-100">
            <h3 class="font-bold text-lg text-slate-800">${acc.number}</h3>
          </div>
          <div class="grid grid-cols-2 gap-4">
            <div>
              <label class="block text-xs font-bold text-sky-700 mb-1">Sub Water Meter</label>
              <select class="map-sub-water w-full rounded border-slate-300 px-3 py-2 text-sm focus:border-sky-500 bg-white" style="border-width:1px;" data-saved="${savedMap.water}">
                ${wOpts}
              </select>
            </div>
            <div>
              <label class="block text-xs font-bold text-indigo-700 mb-1">Sub Electric Meter</label>
              <select class="map-sub-elec w-full rounded border-slate-300 px-3 py-2 text-sm focus:border-indigo-500 bg-white" style="border-width:1px;" data-saved="${savedMap.elec}">
                ${eOpts}
              </select>
            </div>
          </div>
        `;
      }
      container.appendChild(card);
    });

    // Apply saved selections
    document.querySelectorAll('.map-sub-water').forEach(sel => { if(sel.dataset.saved) sel.value = sel.dataset.saved; });
    document.querySelectorAll('.map-sub-elec').forEach(sel => { if(sel.dataset.saved) sel.value = sel.dataset.saved; });
  }

  function gatherMapping() {
    wizardData.mapping = [];
    document.querySelectorAll('.map-card').forEach(card => {
      const accId = card.dataset.accId;
      const isBulk = wizardData.accounts.find(a => a.id === accId)?.isBulk;
      
      if (!isBulk) {
        wizardData.mapping.push({
          account_id: accId,
          water: card.querySelector('.map-sub-water')?.value || '',
          elec: card.querySelector('.map-sub-elec')?.value || ''
        });
      }
    });
  }

  async function saveArchitecture() {
    gatherMapping(); // Final gather
    console.log("Saving Architecture:", wizardData);
    
    try {
      const response = await fetch("{{ url_for('billing_bp.save_global_architecture', property_id=property.id) }}", {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRFToken': '{{ csrf_token() }}'
        },
        body: JSON.stringify(wizardData)
      });
      
      if (response.ok) {
        // Clear draft on successful save!
        localStorage.removeItem(DRAFT_KEY);
        window.location.href = "{{ url_for('billing_bp.learner_dashboard') }}";
      } else {
        const errData = await response.json();
        alert("Server Error: " + (errData.error || "Please ensure all data is valid."));
      }
    } catch (e) {
      console.error(e);
      alert("Network error: " + e.message);
    }
  }
</script>
{% endblock %}"""

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated setup_wizard.html with 5 steps and autosave.")
