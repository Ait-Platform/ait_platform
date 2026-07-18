import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

s2_start = content.find('<!-- STEP 2')
s3_start = content.find('<!-- STEP 3')
s4_start = content.find('<!-- STEP 4')

new_step2 = """<!-- STEP 2: BULK METERS -->
      <div id="step-2" class="wizard-step hidden">
        <div class="flex justify-between items-center mb-2">
            <h2 class="text-xl font-bold text-slate-800">Step 2: Bulk Meter Registry</h2>
        </div>
        <p class="text-sm text-slate-600 mb-6">Enter the master bulk meter number for the entire property. Electrical uses standard tariffs and does not require a bulk meter.</p>
        
        <div class="max-w-md mx-auto bg-sky-50 rounded-xl border border-sky-200 p-4">
            <h3 class="font-bold text-sky-800 mb-3 text-sm flex items-center"><span class="w-2 h-2 rounded-full bg-sky-500 mr-2"></span>Bulk Water Meter</h3>
            <div id="bulk-water-container" class="space-y-3"></div>
        </div>
        <div class="mt-8 flex justify-between">
          <button type="button" onclick="prevStep()" class="px-6 py-2 bg-slate-200 text-slate-700 font-bold rounded-lg hover:bg-slate-300 transition">Back</button>
          <button type="button" onclick="nextStep()" class="px-6 py-2 bg-blue-600 text-white font-bold rounded-lg hover:bg-blue-700 transition">Next Step</button>
        </div>
      </div>

      """

new_step3 = """<!-- STEP 3: SUB METERS -->
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

new_content = content[:s2_start] + new_step2 + new_step3 + content[s4_start:]

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(new_content)

print("Step 2 and Step 3 surgically replaced.")
