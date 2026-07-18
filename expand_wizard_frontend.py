import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update Badges
old_badges = """      <div class="step-badge w-10 h-10 rounded-full flex items-center justify-center font-bold text-lg bg-indigo-600 text-white shadow-md ring-4 ring-indigo-100" data-step="1">1</div>
      <div class="h-1 w-8 bg-slate-200 rounded"></div>
      <div class="step-badge w-10 h-10 rounded-full flex items-center justify-center font-bold text-lg bg-slate-100 text-slate-400" data-step="2">2</div>
      <div class="h-1 w-8 bg-slate-200 rounded"></div>
      <div class="step-badge w-10 h-10 rounded-full flex items-center justify-center font-bold text-lg bg-slate-100 text-slate-400" data-step="3">3</div>
      <div class="h-1 w-8 bg-slate-200 rounded"></div>
      <div class="step-badge w-10 h-10 rounded-full flex items-center justify-center font-bold text-lg bg-slate-100 text-slate-400" data-step="4">4</div>
      <div class="h-1 w-8 bg-slate-200 rounded"></div>
      <div class="step-badge w-10 h-10 rounded-full flex items-center justify-center font-bold text-lg bg-slate-100 text-slate-400" data-step="5">5</div>
      <div class="h-1 w-8 bg-slate-200 rounded"></div>
      <div class="step-badge w-10 h-10 rounded-full flex items-center justify-center font-bold text-lg bg-slate-100 text-slate-400" data-step="6">6</div>
      <div class="h-1 w-8 bg-slate-200 rounded"></div>
      <div class="step-badge w-10 h-10 rounded-full flex items-center justify-center font-bold text-lg bg-slate-100 text-slate-400" data-step="7">7</div>"""

new_badges = """      <div class="step-badge w-8 h-8 rounded-full flex items-center justify-center font-bold text-sm bg-indigo-600 text-white shadow-md ring-2 ring-indigo-100" data-step="1">1</div>
      <div class="h-1 w-4 bg-slate-200 rounded"></div>
      <div class="step-badge w-8 h-8 rounded-full flex items-center justify-center font-bold text-sm bg-slate-100 text-slate-400" data-step="2">2</div>
      <div class="h-1 w-4 bg-slate-200 rounded"></div>
      <div class="step-badge w-8 h-8 rounded-full flex items-center justify-center font-bold text-sm bg-slate-100 text-slate-400" data-step="3">3</div>
      <div class="h-1 w-4 bg-slate-200 rounded"></div>
      <div class="step-badge w-8 h-8 rounded-full flex items-center justify-center font-bold text-sm bg-slate-100 text-slate-400" data-step="4">4</div>
      <div class="h-1 w-4 bg-slate-200 rounded"></div>
      <div class="step-badge w-8 h-8 rounded-full flex items-center justify-center font-bold text-sm bg-slate-100 text-slate-400" data-step="5">5</div>
      <div class="h-1 w-4 bg-slate-200 rounded"></div>
      <div class="step-badge w-8 h-8 rounded-full flex items-center justify-center font-bold text-sm bg-slate-100 text-slate-400" data-step="6">6</div>
      <div class="h-1 w-4 bg-slate-200 rounded"></div>
      <div class="step-badge w-8 h-8 rounded-full flex items-center justify-center font-bold text-sm bg-slate-100 text-slate-400" data-step="7">7</div>
      <div class="h-1 w-4 bg-slate-200 rounded"></div>
      <div class="step-badge w-8 h-8 rounded-full flex items-center justify-center font-bold text-sm bg-slate-100 text-slate-400" data-step="8">8</div>
      <div class="h-1 w-4 bg-slate-200 rounded"></div>
      <div class="step-badge w-8 h-8 rounded-full flex items-center justify-center font-bold text-sm bg-slate-100 text-slate-400" data-step="9">9</div>
      <div class="h-1 w-4 bg-slate-200 rounded"></div>
      <div class="step-badge w-8 h-8 rounded-full flex items-center justify-center font-bold text-sm bg-slate-100 text-slate-400" data-step="10">10</div>"""

content = content.replace(old_badges, new_badges)

# 2. Add the containers for Steps 8, 9, 10 right after Step 7
old_step7_end = """    <!-- STEP 7: MAPPING DASHBOARD -->
    <div id="step-7" class="step-content hidden">
      <h2 class="text-xl font-bold text-slate-800 mb-2">Architectural Mapping</h2>
      <p class="text-slate-500 text-sm mb-6">Assign your defined sub-meters to specific sub-accounts.</p>
      
      <div id="mapping-container" class="space-y-4">
        <!-- Javascript will inject Mapping Dashboard here -->
      </div>
    </div>"""

new_steps = """    <!-- STEP 7: MAPPING DASHBOARD -->
    <div id="step-7" class="step-content hidden">
      <h2 class="text-xl font-bold text-slate-800 mb-2">Architectural Mapping</h2>
      <p class="text-slate-500 text-sm mb-6">Assign your defined sub-meters to specific sub-accounts.</p>
      
      <div id="mapping-container" class="space-y-4">
        <!-- Javascript will inject Mapping Dashboard here -->
      </div>
    </div>

    <!-- STEP 8: ARREARS -->
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

content = content.replace(old_step7_end, new_steps)

# 3. Remove old finalize button from footer
old_footer = """    <div class="mt-8 flex justify-between items-center border-t border-slate-100 pt-6">
      <button type="button" id="btn-prev" onclick="prevStep()" class="hidden px-6 py-2 text-slate-600 font-medium hover:bg-slate-100 rounded-lg transition">Back</button>
      <div class="flex-1 flex justify-end gap-3">
        <span id="autosave-indicator" class="text-xs text-emerald-500 font-medium my-auto mr-4 opacity-0 transition-opacity duration-300">Draft Auto-Saved</span>
        <button type="button" id="btn-next" onclick="nextStep()" class="px-8 py-2 bg-indigo-600 text-white font-bold rounded-lg shadow-sm hover:bg-indigo-700 transition">Next Step</button>
        <button type="button" id="btn-save" onclick="saveArchitecture()" class="hidden px-8 py-2 bg-emerald-600 text-white font-bold rounded-lg shadow-sm hover:bg-emerald-700 transition">Save &amp; Finalize Architecture</button>
      </div>
    </div>"""

new_footer = """    <div class="mt-8 flex justify-between items-center border-t border-slate-100 pt-6">
      <button type="button" id="btn-prev" onclick="prevStep()" class="hidden px-6 py-2 text-slate-600 font-medium hover:bg-slate-100 rounded-lg transition">Back</button>
      <div class="flex-1 flex justify-end gap-3">
        <span id="autosave-indicator" class="text-xs text-emerald-500 font-medium my-auto mr-4 opacity-0 transition-opacity duration-300">Draft Auto-Saved</span>
        <button type="button" id="btn-next" onclick="nextStep()" class="px-8 py-2 bg-indigo-600 text-white font-bold rounded-lg shadow-sm hover:bg-indigo-700 transition">Next Step</button>
      </div>
    </div>"""

content = content.replace(old_footer, new_footer)

# 4. JS updates
content = content.replace('const TOTAL_STEPS = 7;', 'const TOTAL_STEPS = 10;')

# Replace the label logic in updateBadges
old_update_badges = """    if(step === 1) label = "Property Setup Map";
    if(step === 2) label = "Account Registry";
    if(step === 3) label = "Bulk Water Registry";
    if(step === 4) label = "Bulk Electrical Registry";
    if(step === 5) label = "Sub-Meters Registry";
    if(step === 6) label = "Exceptional Cases";
    if(step === 7) label = "Architectural Mapping";"""

new_update_badges = """    if(step === 1) label = "Property Setup";
    if(step === 2) label = "Accounts";
    if(step === 3) label = "Bulk Water";
    if(step === 4) label = "Bulk Elec";
    if(step === 5) label = "Sub-Meters";
    if(step === 6) label = "Exceptions";
    if(step === 7) label = "Mapping";
    if(step === 8) label = "Arrears";
    if(step === 9) label = "Arrangements";
    if(step === 10) label = "Owners";"""
content = content.replace(old_update_badges, new_update_badges)

# Update showStep buttons
old_show_step_buttons = """    // Buttons
    document.getElementById('btn-prev').classList.toggle('hidden', currentStep === 1);
    document.getElementById('btn-next').classList.toggle('hidden', currentStep === TOTAL_STEPS);
    document.getElementById('btn-save').classList.toggle('hidden', currentStep !== TOTAL_STEPS);"""
new_show_step_buttons = """    // Buttons
    document.getElementById('btn-prev').classList.toggle('hidden', currentStep === 1);
    document.getElementById('btn-next').classList.toggle('hidden', currentStep === TOTAL_STEPS);
    
    // Build dynamic dashboards
    if(step === 7) buildMappingDashboard();
    if(step === 8) buildArrearsDashboard();
    if(step === 9) buildArrangementsDashboard();
    if(step === 10) buildOwnersDashboard();"""
content = content.replace(old_show_step_buttons, new_show_step_buttons)
content = content.replace("    if(step === 7) buildMappingDashboard();", "") # remove the original one

# Update nextStep to trigger gathers
old_nextstep = """    if (currentStep === 5) { 
        if(!gatherMeters('sub-water')) return; 
        if(!gatherMeters('sub-elec')) return; 
    }
    
    // Global Meter Validation (Only block when finishing all meter steps)
    if (currentStep === 5) {
        if (!validateGlobalMeters()) return;
    }
    
    if (currentStep === 6) { gatherExceptions(); }
    if (currentStep === 7) { gatherMapping(); }"""

new_nextstep = """    if (currentStep === 5) { 
        if(!gatherMeters('sub-water')) return; 
        if(!gatherMeters('sub-elec')) return; 
    }
    
    // Global Meter Validation (Only block when finishing all meter steps)
    if (currentStep === 5) {
        if (!validateGlobalMeters()) return;
    }
    
    if (currentStep === 6) { gatherExceptions(); }
    if (currentStep === 7) { gatherMapping(); }
    if (currentStep === 8) { gatherArrears(); }
    if (currentStep === 9) { gatherArrangements(); }
    if (currentStep === 10) { gatherOwners(); }"""
content = content.replace(old_nextstep, new_nextstep)

# Update triggerAutoSave
old_trigger = """      gatherMeters('sub-elec', true);
      gatherExceptions();
      gatherMapping();
      
      fetch"""
new_trigger = """      gatherMeters('sub-elec', true);
      gatherExceptions();
      gatherMapping();
      gatherArrears();
      gatherArrangements();
      gatherOwners();
      
      fetch"""
content = content.replace(old_trigger, new_trigger)

# Inject the new build/gather functions
new_functions = """
  // --- ARREARS (STEP 8) ---
  function buildArrearsDashboard() {
    const container = document.getElementById('arrears-container');
    container.innerHTML = '';
    wizardData.accounts.forEach(acc => {
      if(!acc.number) return;
      const savedArr = (wizardData.arrears && wizardData.arrears.find(x => x.account_id === acc.id)) || { amount: 0 };
      container.innerHTML += `
        <div class="flex items-center justify-between p-3 bg-white border border-slate-200 rounded-lg shadow-sm arr-card" data-acc-id="${acc.id}">
          <span class="font-bold text-slate-700 w-1/3">${acc.number} ${acc.isBulk ? '<span class="text-[10px] bg-amber-100 text-amber-800 px-1 rounded ml-1">BULK</span>' : ''}</span>
          <div class="w-1/3 relative">
            <span class="absolute left-3 top-2 text-slate-400">R</span>
            <input type="number" class="arr-amount w-full rounded border-slate-300 pl-8 pr-3 py-1.5 text-sm outline-none" value="${savedArr.amount}" onchange="triggerAutoSave()" step="0.01">
          </div>
        </div>
      `;
    });
  }

  function gatherArrears() {
    if (currentStep < 8) return;
    wizardData.arrears = [];
    document.querySelectorAll('.arr-card').forEach(card => {
      wizardData.arrears.push({
        account_id: card.dataset.accId,
        amount: parseFloat(card.querySelector('.arr-amount').value) || 0
      });
    });
  }

  // --- ARRANGEMENTS (STEP 9) ---
  function buildArrangementsDashboard() {
    const container = document.getElementById('arrangements-container');
    container.innerHTML = '';
    wizardData.accounts.forEach(acc => {
      if(!acc.number) return;
      const savedArg = (wizardData.arrangements && wizardData.arrangements.find(x => x.account_id === acc.id)) || { amount: 0, duration: 0 };
      container.innerHTML += `
        <div class="flex items-center space-x-4 p-3 bg-white border border-slate-200 rounded-lg shadow-sm arg-card" data-acc-id="${acc.id}">
          <span class="font-bold text-slate-700 w-1/4">${acc.number}</span>
          <div class="flex-1 relative">
            <span class="absolute left-3 top-2 text-slate-400 text-xs">Amount R</span>
            <input type="number" class="arg-amount w-full rounded border-slate-300 pl-16 pr-2 py-1.5 text-sm outline-none" value="${savedArg.amount}" onchange="triggerAutoSave()" step="0.01">
          </div>
          <div class="flex-1 relative">
            <span class="absolute left-3 top-2 text-slate-400 text-xs">Months</span>
            <input type="number" class="arg-duration w-full rounded border-slate-300 pl-14 pr-2 py-1.5 text-sm outline-none" value="${savedArg.duration}" onchange="triggerAutoSave()" step="1">
          </div>
        </div>
      `;
    });
  }

  function gatherArrangements() {
    if (currentStep < 9) return;
    wizardData.arrangements = [];
    document.querySelectorAll('.arg-card').forEach(card => {
      wizardData.arrangements.push({
        account_id: card.dataset.accId,
        amount: parseFloat(card.querySelector('.arg-amount').value) || 0,
        duration: parseInt(card.querySelector('.arg-duration').value) || 0
      });
    });
  }

  // --- OWNERS (STEP 10) ---
  function buildOwnersDashboard() {
    const container = document.getElementById('owners-container');
    container.innerHTML = '';
    wizardData.accounts.forEach(acc => {
      if(!acc.number) return;
      const savedOwn = (wizardData.owners && wizardData.owners.find(x => x.account_id === acc.id)) || { name: acc.owner || '', email: '' };
      container.innerHTML += `
        <div class="flex flex-col space-y-2 p-3 bg-white border border-slate-200 rounded-lg shadow-sm own-card" data-acc-id="${acc.id}">
          <span class="font-bold text-slate-700">${acc.number}</span>
          <div class="flex space-x-2">
            <input type="text" class="own-name w-1/2 rounded border-slate-300 px-3 py-1.5 text-sm outline-none" placeholder="Owner Name" value="${savedOwn.name}" onchange="triggerAutoSave()">
            <input type="email" class="own-email w-1/2 rounded border-slate-300 px-3 py-1.5 text-sm outline-none" placeholder="Owner Email" value="${savedOwn.email}" onchange="triggerAutoSave()">
          </div>
        </div>
      `;
    });
  }

  function gatherOwners() {
    if (currentStep < 10) return;
    wizardData.owners = [];
    document.querySelectorAll('.own-card').forEach(card => {
      wizardData.owners.push({
        account_id: card.dataset.accId,
        name: card.querySelector('.own-name').value.trim(),
        email: card.querySelector('.own-email').value.trim()
      });
    });
  }
"""

content = content.replace('  async function saveArchitecture() {', new_functions + '\n  async function saveArchitecture() {')
content = content.replace("gatherMapping(); // Final gather", "gatherMapping(); gatherArrears(); gatherArrangements(); gatherOwners();")

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Expanded wizard to 10 steps in setup_wizard.html")
