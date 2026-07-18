import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace Step 2 Block
pattern_step2 = r'<div id="step-2" class="wizard-step hidden">.*?<div class="mt-8 flex justify-between">'
replacement_step2 = """<div id="step-2" class="wizard-step hidden">
        <div class="flex justify-between items-center mb-2">
            <h2 class="text-xl font-bold text-slate-800">Step 2: Bulk Meter Registry</h2>
        </div>
        <p class="text-sm text-slate-600 mb-6">Enter the master bulk meter number for the entire property. Electrical uses standard tariffs and does not require a bulk meter.</p>
        
        <div class="max-w-md mx-auto bg-sky-50 rounded-xl border border-sky-200 p-4">
            <h3 class="font-bold text-sky-800 mb-3 text-sm flex items-center"><span class="w-2 h-2 rounded-full bg-sky-500 mr-2"></span>Bulk Water Meter</h3>
            <div id="bulk-water-container" class="space-y-3"></div>
        </div>
        <div class="mt-8 flex justify-between">"""
content = re.sub(pattern_step2, replacement_step2, content, flags=re.DOTALL)

# Replace Step 3 Block
pattern_step3 = r'<div id="step-3" class="wizard-step hidden">.*?<div class="mt-8 flex justify-between">'
replacement_step3 = """<div id="step-3" class="wizard-step hidden">
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
        <div class="mt-8 flex justify-between">"""
content = re.sub(pattern_step3, replacement_step3, content, flags=re.DOTALL)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Step 2 and 3 HTML properly locked down.")
