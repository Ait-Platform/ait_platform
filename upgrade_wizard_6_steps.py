import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update TOTAL_STEPS
content = content.replace("const TOTAL_STEPS = 5;", "const TOTAL_STEPS = 6;")

# 2. Update step ids in HTML
content = content.replace('id="step-5"', 'id="step-6"')
content = content.replace('<!-- STEP 5: MASTER MAPPING -->', '<!-- STEP 6: MASTER MAPPING -->')
content = content.replace('Step 5: Master Mapping', 'Step 6: Master Mapping')

content = content.replace('id="step-4"', 'id="step-5"')
content = content.replace('<!-- STEP 4: EXCEPTIONAL CASES -->', '<!-- STEP 5: EXCEPTIONAL CASES -->')
content = content.replace('Step 4: Exceptional', 'Step 5: Exceptional')

# 3. Extract and rebuild Step 2, 3, and 4
s2_start = content.find('<!-- STEP 2')
s4_start = content.find('<!-- STEP 5: EXCEPTIONAL') # This used to be STEP 4, we just renamed it above

new_steps = """<!-- STEP 2: BULK WATER -->
      <div id="step-2" class="wizard-step hidden">
        <div class="flex justify-between items-center mb-2">
            <h2 class="text-xl font-bold text-slate-800">Step 2: Bulk Water Registry</h2>
        </div>
        <p class="text-sm text-slate-600 mb-6">Enter the master bulk water meter numbers for the entire property. These are strictly locked based on the Property Map.</p>
        
        <div class="max-w-md mx-auto bg-sky-50 rounded-xl border border-sky-200 p-4">
            <h3 class="font-bold text-sky-800 mb-3 text-sm flex items-center"><span class="w-2 h-2 rounded-full bg-sky-500 mr-2"></span>Bulk Water Meter</h3>
            <div id="bulk-water-container" class="space-y-3"></div>
        </div>
      </div>

      <!-- STEP 3: BULK ELECTRICAL -->
      <div id="step-3" class="wizard-step hidden">
        <div class="flex justify-between items-center mb-2">
            <h2 class="text-xl font-bold text-slate-800">Step 3: Bulk Electrical Registry</h2>
        </div>
        <p class="text-sm text-slate-600 mb-6">Enter the master bulk electrical meter numbers for the entire property. These are strictly locked based on the Property Map.</p>
        
        <div class="max-w-md mx-auto bg-indigo-50 rounded-xl border border-indigo-200 p-4">
            <h3 class="font-bold text-indigo-800 mb-3 text-sm flex items-center"><span class="w-2 h-2 rounded-full bg-indigo-500 mr-2"></span>Bulk Electrical Meter</h3>
            <div id="bulk-elec-container" class="space-y-3"></div>
        </div>
      </div>

      <!-- STEP 4: SUB METERS -->
      <div id="step-4" class="wizard-step hidden">
        <div class="flex justify-between items-center mb-2">
            <h2 class="text-xl font-bold text-slate-800">Step 4: Sub-Meters Registry</h2>
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
      </div>

      """

content = content[:s2_start] + new_steps + content[s4_start:]

# 4. Update nextStep() logic
old_nextstep = """  function nextStep() {
    if (currentStep === 1) { if (!gatherAccounts()) return; }
    if (currentStep === 2) { gatherMeters('bulk-water'); gatherMeters('bulk-elec'); }
    if (currentStep === 3) { gatherMeters('sub-water'); gatherMeters('sub-elec'); }
    if (currentStep === 4) { gatherExceptions(); }"""

new_nextstep = """  function nextStep() {
    if (currentStep === 1) { if (!gatherAccounts()) return; }
    if (currentStep === 2) { gatherMeters('bulk-water'); }
    if (currentStep === 3) { gatherMeters('bulk-elec'); }
    if (currentStep === 4) { gatherMeters('sub-water'); gatherMeters('sub-elec'); }
    if (currentStep === 5) { gatherExceptions(); }"""

content = content.replace(old_nextstep, new_nextstep)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Wizard upgraded to 6 steps.")
