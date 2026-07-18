import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update JS Constants
old_consts = """  const IS_BULK = {{ 'true' if property.is_bulk_metered else 'false' }};
  const EXPECTED_BULK_WATER = IS_BULK && EXPECTED_WATER > 0 ? 1 : 0;
  const EXPECTED_SUB_WATER = EXPECTED_WATER - EXPECTED_BULK_WATER;
  const EXPECTED_SUB_ELEC = EXPECTED_ELEC;"""

new_consts = """  const IS_BULK_WATER = {{ 'true' if property.is_bulk_water else 'false' }};
  const IS_BULK_ELEC = {{ 'true' if property.is_bulk_elec else 'false' }};
  const EXPECTED_BULK_WATER = IS_BULK_WATER && EXPECTED_WATER > 0 ? 1 : 0;
  const EXPECTED_SUB_WATER = EXPECTED_WATER - EXPECTED_BULK_WATER;
  
  const EXPECTED_BULK_ELEC = IS_BULK_ELEC && EXPECTED_ELEC > 0 ? 1 : 0;
  const EXPECTED_SUB_ELEC = EXPECTED_ELEC - EXPECTED_BULK_ELEC;"""
content = content.replace(old_consts, new_consts)

# 2. Update initializeBlank()
old_init = """  function initializeBlank() {
      for(let i=0; i<EXPECTED_ACCOUNTS; i++) addAccountRow();
      const firstRadio = document.querySelector('input[name="bulk_idx"]');
      if (firstRadio) firstRadio.checked = true;

      for(let i=0; i<EXPECTED_BULK_WATER; i++) addMeterRow('bulk-water');
      for(let i=0; i<EXPECTED_SUB_WATER; i++) addMeterRow('sub-water');
      for(let i=0; i<EXPECTED_SUB_ELEC; i++) addMeterRow('sub-elec');
      
      addExceptionRow();
  }"""
new_init = """  function initializeBlank() {
      for(let i=0; i<EXPECTED_ACCOUNTS; i++) addAccountRow();
      const firstRadio = document.querySelector('input[name="bulk_idx"]');
      if (firstRadio) firstRadio.checked = true;

      for(let i=0; i<EXPECTED_BULK_WATER; i++) addMeterRow('bulk-water');
      for(let i=0; i<EXPECTED_BULK_ELEC; i++) addMeterRow('bulk-elec');
      for(let i=0; i<EXPECTED_SUB_WATER; i++) addMeterRow('sub-water');
      for(let i=0; i<EXPECTED_SUB_ELEC; i++) addMeterRow('sub-elec');
      
      addExceptionRow();
  }"""
content = content.replace(old_init, new_init)

# 3. Update loadDraft()
old_load = """        // Strict Meter Row Generation
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
        }"""
new_load = """        // Strict Meter Row Generation
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
        }"""
content = content.replace(old_load, new_load)


# 4. Surgically Replace Step 2 and Step 3 HTML
s2_start = content.find('<!-- STEP 2')
s3_start = content.find('<!-- STEP 3')
s4_start = content.find('<!-- STEP 4')

new_step2_html = """<!-- STEP 2: BULK METERS -->
      <div id="step-2" class="wizard-step hidden">
        <div class="flex justify-between items-center mb-2">
            <h2 class="text-xl font-bold text-slate-800">Step 2: Bulk Meter Registry</h2>
        </div>
        <p class="text-sm text-slate-600 mb-6">Enter the master bulk meter numbers for the entire property. These are strictly locked based on the Property Map.</p>
        
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div class="bg-sky-50 rounded-xl border border-sky-200 p-4">
            <h3 class="font-bold text-sky-800 mb-3 text-sm flex items-center"><span class="w-2 h-2 rounded-full bg-sky-500 mr-2"></span>Bulk Water Meter</h3>
            <div id="bulk-water-container" class="space-y-3"></div>
          </div>
          <div class="bg-indigo-50 rounded-xl border border-indigo-200 p-4">
            <h3 class="font-bold text-indigo-800 mb-3 text-sm flex items-center"><span class="w-2 h-2 rounded-full bg-indigo-500 mr-2"></span>Bulk Electrical Meter</h3>
            <div id="bulk-elec-container" class="space-y-3"></div>
          </div>
        </div>
        <div class="mt-8 flex justify-between">
          <button type="button" onclick="prevStep()" class="px-6 py-2 bg-slate-200 text-slate-700 font-bold rounded-lg hover:bg-slate-300 transition">Back</button>
          <button type="button" onclick="nextStep()" class="px-6 py-2 bg-blue-600 text-white font-bold rounded-lg hover:bg-blue-700 transition">Next Step</button>
        </div>
      </div>

      """

new_step3_html = """<!-- STEP 3: SUB METERS -->
      <div id="step-3" class="wizard-step hidden">
        <div class="flex justify-between items-center mb-2">
            <h2 class="text-xl font-bold text-slate-800">Step 3: Sub-Meters Registry</h2>
        </div>
        <p class="text-sm text-slate-600 mb-6">These rows are strictly locked based on the Total Water and Total Electric meters defined in your Property Map.</p>
        
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div class="bg-sky-50 rounded-xl border border-sky-200 p-4">
            <h3 class="font-bold text-sky-800 mb-3 text-sm flex items-center"><span class="w-2 h-2 rounded-full bg-sky-500 mr-2"></span>Sub Water Meters</h3>
            <div id="sub-water-container" class="space-y-3"></div>
          </div>
          <div class="bg-indigo-50 rounded-xl border border-indigo-200 p-4">
            <h3 class="font-bold text-indigo-800 mb-3 text-sm flex items-center"><span class="w-2 h-2 rounded-full bg-indigo-500 mr-2"></span>Sub Electric Meters</h3>
            <div id="sub-elec-container" class="space-y-3"></div>
          </div>
        </div>
        <div class="mt-8 flex justify-between">
          <button type="button" onclick="prevStep()" class="px-6 py-2 bg-slate-200 text-slate-700 font-bold rounded-lg hover:bg-slate-300 transition">Back</button>
          <button type="button" onclick="nextStep()" class="px-6 py-2 bg-blue-600 text-white font-bold rounded-lg hover:bg-blue-700 transition">Next Step</button>
        </div>
      </div>

      """

new_content = content[:s2_start] + new_step2_html + new_step3_html + content[s4_start:]

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(new_content)

print("Wizard JS logic and HTML structure reverted to dual-columns with strict locking.")
