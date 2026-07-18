import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update Badges HTML
old_badges = """        <div class="flex space-x-2 text-[11px]">
          <span id="step-1-badge" class="px-2 py-1 rounded-full font-bold bg-blue-100 text-blue-700">1. Property</span>
          <span id="step-2-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">2. Accounts</span>
          <span id="step-3-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">3. Bulk Water</span>
          <span id="step-4-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">4. Bulk Elec</span>
          <span id="step-5-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">5. Sub Meters</span>
          <span id="step-6-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">6. Exceptions</span>
          <span id="step-7-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">7. Mapping</span>
          <span id="step-8-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">8. Arrears</span>
          <span id="step-9-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">9. Arrangements</span>
          <span id="step-10-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">10. Owners</span>
        </div>"""

new_badges = """        <div class="flex space-x-2 text-[11px] overflow-x-auto pb-2 custom-scrollbar">
          <span id="step-1-badge" class="px-2 py-1 rounded-full font-bold bg-blue-100 text-blue-700 whitespace-nowrap">1. Property</span>
          <span id="step-2-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400 whitespace-nowrap">2. Accounts</span>
          <span id="step-3-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400 whitespace-nowrap">3. Bulk Water</span>
          <span id="step-4-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400 whitespace-nowrap">4. Bulk Elec</span>
          <span id="step-5-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400 whitespace-nowrap">5. Sub Meters</span>
          <span id="step-6-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400 whitespace-nowrap">6. Exceptions</span>
          <span id="step-7-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400 whitespace-nowrap">7. Mapping</span>
          <span id="step-8-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400 whitespace-nowrap">8. Readings</span>
          <span id="step-9-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400 whitespace-nowrap">9. Arrears</span>
          <span id="step-10-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400 whitespace-nowrap">10. Arrangements</span>
          <span id="step-11-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400 whitespace-nowrap">11. Owners</span>
        </div>
        <style>
          .custom-scrollbar::-webkit-scrollbar { height: 4px; }
          .custom-scrollbar::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 4px; }
        </style>"""

content = content.replace(old_badges, new_badges)

# 2. Update the HTML Containers
old_steps = """    <!-- STEP 8: ARREARS -->
    <div id="step-8" class="step-content hidden">
      <h2 class="text-xl font-bold text-slate-800 mb-2">Arrears Registry</h2>
      <p class="text-slate-500 text-sm mb-6">Capture any outstanding arrears for each account.</p>
      <div id="arrears-container" class="space-y-3"></div>
    </div>

    <!-- STEP 9: ARRANGEMENTS -->
    <div id="step-9" class="step-content hidden">
      <h2 class="text-xl font-bold text-slate-800 mb-2">Arrangements Registry</h2>
      <p class="text-slate-500 text-sm mb-6">Capture payment arrangements and durations.</p>
      <div id="arrangements-container" class="space-y-3"></div>
    </div>

    <!-- STEP 10: OWNERS & FINALIZATION -->
    <div id="step-10" class="step-content hidden">
      <h2 class="text-xl font-bold text-slate-800 mb-2">Owner Registry</h2>
      <p class="text-slate-500 text-sm mb-6">Assign owner details to each account to complete the setup.</p>
      <div id="owners-container" class="space-y-3 mb-8"></div>
      
      <div class="p-6 bg-emerald-50 rounded-xl border border-emerald-200 text-center">
          <h3 class="font-bold text-emerald-800 text-lg mb-2">Architecture Setup Complete</h3>
          <p class="text-emerald-700 text-sm mb-6">You have successfully mapped out the property, registered all meters, handled exceptions, and documented financial histories.</p>
          <button type="button" id="btn-save" onclick="saveArchitecture()" class="px-8 py-3 bg-emerald-600 text-white font-bold rounded-lg shadow-sm hover:bg-emerald-700 transition">Save &amp; Finalize Architecture</button>
      </div>
    </div>"""

new_steps = """    <!-- STEP 8: INITIAL READINGS -->
    <div id="step-8" class="step-content hidden">
      <h2 class="text-xl font-bold text-slate-800 mb-2">Initial Readings</h2>
      <p class="text-slate-500 text-sm mb-6">Set the absolute ground-zero starting readings and dates for every meter in the architecture.</p>
      <div id="readings-container" class="space-y-3"></div>
    </div>

    <!-- STEP 9: ARREARS -->
    <div id="step-9" class="step-content hidden">
      <h2 class="text-xl font-bold text-slate-800 mb-2">Arrears Registry</h2>
      <p class="text-slate-500 text-sm mb-6">Capture any outstanding arrears for each account.</p>
      <div id="arrears-container" class="space-y-3"></div>
    </div>

    <!-- STEP 10: ARRANGEMENTS -->
    <div id="step-10" class="step-content hidden">
      <h2 class="text-xl font-bold text-slate-800 mb-2">Arrangements Registry</h2>
      <p class="text-slate-500 text-sm mb-6">Capture payment arrangements and durations.</p>
      <div id="arrangements-container" class="space-y-3"></div>
    </div>

    <!-- STEP 11: OWNERS & FINALIZATION -->
    <div id="step-11" class="step-content hidden">
      <h2 class="text-xl font-bold text-slate-800 mb-2">Owner Registry</h2>
      <p class="text-slate-500 text-sm mb-6">Assign owner details to each account to complete the setup.</p>
      <div id="owners-container" class="space-y-3 mb-8"></div>
      
      <div class="p-6 bg-emerald-50 rounded-xl border border-emerald-200 text-center">
          <h3 class="font-bold text-emerald-800 text-lg mb-2">Architecture Setup Complete</h3>
          <p class="text-emerald-700 text-sm mb-6">You have successfully mapped out the property, registered all meters, set initial readings, and documented financial histories.</p>
          <button type="button" id="btn-save" onclick="saveArchitecture()" class="px-8 py-3 bg-emerald-600 text-white font-bold rounded-lg shadow-sm hover:bg-emerald-700 transition">Save &amp; Finalize Architecture</button>
      </div>
    </div>"""

content = content.replace(old_steps, new_steps)

# 3. Update TOTAL_STEPS
content = content.replace("const TOTAL_STEPS = 10;", "const TOTAL_STEPS = 11;")

# 4. Update JS label array
old_labels = "const labels = ['1. Property', '2. Accounts', '3. Bulk Water', '4. Bulk Elec', '5. Sub Meters', '6. Exceptions', '7. Mapping', '8. Arrears', '9. Arrangements', '10. Owners'];"
new_labels = "const labels = ['1. Property', '2. Accounts', '3. Bulk Water', '4. Bulk Elec', '5. Sub Meters', '6. Exceptions', '7. Mapping', '8. Readings', '9. Arrears', '10. Arrangements', '11. Owners'];"
content = content.replace(old_labels, new_labels)

# 5. Update showStep routing
old_show_step_routing = """    if(step === 6) updateExceptionDropdowns();
    if(step === 7) buildMappingDashboard();
    if(step === 8) buildArrearsDashboard();
    if(step === 9) buildArrangementsDashboard();
    if(step === 10) buildOwnersDashboard();"""

new_show_step_routing = """    if(step === 6) updateExceptionDropdowns();
    if(step === 7) buildMappingDashboard();
    if(step === 8) buildReadingsDashboard();
    if(step === 9) buildArrearsDashboard();
    if(step === 10) buildArrangementsDashboard();
    if(step === 11) buildOwnersDashboard();"""
content = content.replace(old_show_step_routing, new_show_step_routing)

# 6. Add buildReadingsDashboard and gatherInitialReadings to JS
new_js_functions = """
  // --- INITIAL READINGS (STEP 8) ---
  function buildReadingsDashboard() {
    const container = document.getElementById('readings-container');
    container.innerHTML = '';
    
    const allMeters = [
        ...(wizardData.bulkWater || []).map(m => ({ ...m, type: 'Bulk Water' })),
        ...(wizardData.bulkElec || []).map(m => ({ ...m, type: 'Bulk Elec' })),
        ...(wizardData.subWater || []).map(m => ({ ...m, type: 'Sub Water' })),
        ...(wizardData.subElec || []).map(m => ({ ...m, type: 'Sub Elec' }))
    ];
    
    wizardData.initialReadings = wizardData.initialReadings || [];
    
    if (allMeters.length === 0) {
      container.innerHTML = '<div class="p-4 text-slate-500 italic">No meters defined in architecture.</div>';
      return;
    }
    
    allMeters.forEach(m => {
      if(!m.number) return;
      const savedReading = wizardData.initialReadings.find(x => x.meter_number === m.number) || { value: '', date: '' };
      
      let badgeColor = m.type.includes('Water') ? 'bg-sky-100 text-sky-800' : 'bg-indigo-100 text-indigo-800';
      if (m.type.includes('Bulk')) badgeColor += ' border border-amber-300 shadow-sm';
      
      container.innerHTML += `
        <div class="flex items-center justify-between p-3 bg-white border border-slate-200 rounded-lg shadow-sm read-card" data-meter-num="${m.number}">
          <div class="w-1/3 flex flex-col">
            <span class="font-bold text-slate-800 text-base">${m.number}</span>
            <span class="text-[10px] uppercase font-bold px-2 py-0.5 mt-1 rounded inline-block w-max ${badgeColor}">${m.type}</span>
          </div>
          <div class="w-1/3 px-2">
            <label class="block text-[10px] uppercase font-bold text-slate-500 mb-1">Starting Reading</label>
            <input type="number" class="read-value w-full rounded border-slate-300 px-3 py-1.5 text-sm outline-none focus:border-blue-500" value="${savedReading.value}" onchange="triggerAutoSave()" step="0.01">
          </div>
          <div class="w-1/3 pl-2">
            <label class="block text-[10px] uppercase font-bold text-slate-500 mb-1">Reading Date</label>
            <input type="date" class="read-date w-full rounded border-slate-300 px-3 py-1.5 text-sm outline-none focus:border-blue-500" value="${savedReading.date}" onchange="triggerAutoSave()">
          </div>
        </div>
      `;
    });
  }
  
  function gatherInitialReadings() {
    wizardData.initialReadings = [];
    document.querySelectorAll('.read-card').forEach(card => {
      wizardData.initialReadings.push({
        meter_number: card.dataset.meterNum,
        value: parseFloat(card.querySelector('.read-value').value), // Can be NaN if empty
        date: card.querySelector('.read-date').value
      });
    });
  }
"""

content = content.replace("// --- ARREARS (STEP 8) ---", new_js_functions + "\n  // --- ARREARS (STEP 9) ---")

# 7. Add gatherInitialReadings to triggerAutoSave and nextStep
old_trigger = """      gatherExceptions();
      gatherMapping();
      gatherArrears();"""
new_trigger = """      gatherExceptions();
      gatherMapping();
      gatherInitialReadings();
      gatherArrears();"""
content = content.replace(old_trigger, new_trigger)

old_save_gather = "gatherMapping(); gatherArrears(); gatherArrangements(); gatherOwners();"
new_save_gather = "gatherMapping(); gatherInitialReadings(); gatherArrears(); gatherArrangements(); gatherOwners();"
content = content.replace(old_save_gather, new_save_gather)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Setup wizard expanded to 11 steps.")
