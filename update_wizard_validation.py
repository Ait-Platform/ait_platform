import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Add constants
old_consts = """  const DRAFT_KEY = 'billing_wizard_draft_{{ property.id }}';
  const EXPECTED_ACCOUNTS = {{ property.expected_bills|default(8) }};"""
new_consts = """  const EXPECTED_ACCOUNTS = {{ property.expected_bills|default(8) }};
  const EXPECTED_WATER = {{ property.expected_water_meters|default(0) }};
  const EXPECTED_ELEC = {{ property.expected_elec_meters|default(0) }};"""
if "EXPECTED_WATER" not in content:
    content = content.replace("  const EXPECTED_ACCOUNTS = {{ property.expected_bills|default(8) }};", new_consts)

# Add counters to Step 2
old_step2_head = """      <div id="step-2" class="wizard-step hidden">
        <h2 class="text-xl font-bold text-slate-800 mb-2">Step 2: Bulk Meters Registry</h2>"""
new_step2_head = """      <div id="step-2" class="wizard-step hidden">
        <div class="flex justify-between items-center mb-2">
            <h2 class="text-xl font-bold text-slate-800">Step 2: Bulk Meters Registry</h2>
            <div class="flex space-x-3 text-sm font-bold">
                <span class="px-3 py-1 bg-sky-100 text-sky-800 rounded-full" id="s2-water-count">Water: 0 / {{ property.expected_water_meters|default(0) }}</span>
                <span class="px-3 py-1 bg-indigo-100 text-indigo-800 rounded-full" id="s2-elec-count">Elec: 0 / {{ property.expected_elec_meters|default(0) }}</span>
            </div>
        </div>"""
content = content.replace(old_step2_head, new_step2_head)

# Add counters to Step 3
old_step3_head = """      <div id="step-3" class="wizard-step hidden">
        <h2 class="text-xl font-bold text-slate-800 mb-2">Step 3: Sub-Meters Registry</h2>"""
new_step3_head = """      <div id="step-3" class="wizard-step hidden">
        <div class="flex justify-between items-center mb-2">
            <h2 class="text-xl font-bold text-slate-800">Step 3: Sub-Meters Registry</h2>
            <div class="flex space-x-3 text-sm font-bold">
                <span class="px-3 py-1 bg-sky-100 text-sky-800 rounded-full" id="s3-water-count">Water: 0 / {{ property.expected_water_meters|default(0) }}</span>
                <span class="px-3 py-1 bg-indigo-100 text-indigo-800 rounded-full" id="s3-elec-count">Elec: 0 / {{ property.expected_elec_meters|default(0) }}</span>
            </div>
        </div>"""
content = content.replace(old_step3_head, new_step3_head)

# Update triggerAutoSave / mutation logic to update live counters
# The easiest way to update live counters is inside `triggerAutoSave()` which runs whenever an input changes.
old_autosave = """  let saveTimeout = null;
  function triggerAutoSave() {
    if(saveTimeout) clearTimeout(saveTimeout);
    saveTimeout = setTimeout(() => {"""
new_autosave = """  let saveTimeout = null;
  function updateLiveCounters() {
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
    if(saveTimeout) clearTimeout(saveTimeout);
    saveTimeout = setTimeout(() => {"""
content = content.replace(old_autosave, new_autosave)

# Update nextStep to block moving past Step 3 if validation fails
old_nextstep = """  function nextStep() {
    if (currentStep === 1) { if (!gatherAccounts()) return; }
    if (currentStep === 2) { gatherMeters('bulk-water'); gatherMeters('bulk-elec'); }
    if (currentStep === 3) { gatherMeters('sub-water'); gatherMeters('sub-elec'); }
    if (currentStep === 4) { gatherExceptions(); }"""
    
new_nextstep = """  function nextStep() {
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
content = content.replace(old_nextstep, new_nextstep)

# Ensure updateLiveCounters runs on load
old_loaddraft = """  document.addEventListener('DOMContentLoaded', () => {
    loadDraft();"""
new_loaddraft = """  document.addEventListener('DOMContentLoaded', () => {
    loadDraft();
    updateLiveCounters();"""
content = content.replace(old_loaddraft, new_loaddraft)


with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated setup_wizard.html with validation counters")
