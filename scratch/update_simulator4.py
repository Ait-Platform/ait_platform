import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# 1. Flatten app-view-31
pattern = r'<div class="app-view hidden overflow-y-auto pb-20" id="app-view-31">.*?<div class="space-y-4 text-sm">'
replacement = '''<div class="app-view hidden overflow-y-auto pb-20" id="app-view-31">
    <div class="p-4 mb-2">
      <h3 class="text-xl font-bold mb-1 text-slate-800"><i class="fas fa-clipboard-check text-indigo-500 mr-2"></i>Evaluation of Facilitator</h3>
      <p class="text-xs text-slate-500 border-b pb-4">Rate the physical demonstration of the LITRE sequence (0: Not demonstrated, 3: Clear/Strong)</p>
    </div>
    <div class="bg-white px-4 pb-4">
      <div class="space-y-4 text-sm">'''

text = re.sub(pattern, replacement, text, flags=re.DOTALL)

# 2. Fix the defaults
text = text.replace('class="flex-1 text-center py-2 bg-indigo-100 border border-indigo-300 rounded cursor-pointer"><input type="radio" name="rubric_vocalization" value="3" checked class="mr-1"> 3</label>', 'class="flex-1 text-center py-2 bg-slate-50 border border-slate-200 rounded cursor-pointer hover:bg-indigo-50"><input type="radio" name="rubric_vocalization" value="3" class="mr-1"> 3</label>')
text = text.replace('class="flex-1 text-center py-2 bg-indigo-100 border border-indigo-300 rounded cursor-pointer"><input type="radio" name="rubric_positioning" value="3" checked class="mr-1"> 3</label>', 'class="flex-1 text-center py-2 bg-slate-50 border border-slate-200 rounded cursor-pointer hover:bg-indigo-50"><input type="radio" name="rubric_positioning" value="3" class="mr-1"> 3</label>')
text = text.replace('class="flex-1 text-center py-2 bg-indigo-100 border border-indigo-300 rounded cursor-pointer"><input type="radio" name="rubric_pacing" value="3" checked class="mr-1"> 3</label>', 'class="flex-1 text-center py-2 bg-slate-50 border border-slate-200 rounded cursor-pointer hover:bg-indigo-50"><input type="radio" name="rubric_pacing" value="3" class="mr-1"> 3</label>')

# 3. Add Validation to nextStep
pattern_next = r'function nextStep\(\) \{\s*if \(currentStepIndex < simSteps\.length - 1\) \{'
replacement_next = '''function nextStep() {
        // Validation for Slide 31
        const currentStepData = simSteps[currentStepIndex];
        if (currentStepData && currentStepData.appView === 31) {
            const v = document.querySelector('input[name="rubric_vocalization"]:checked');
            const p = document.querySelector('input[name="rubric_positioning"]:checked');
            const pa = document.querySelector('input[name="rubric_pacing"]:checked');
            
            if (!v || !p || !pa) {
                alert("Please complete the entire Facilitator Evaluation rubric before proceeding.");
                
                // Highlight missing
                if (!v) document.querySelectorAll('input[name="rubric_vocalization"]')[0].closest('.flex-col').classList.add('border', 'border-red-500', 'p-2', 'rounded');
                if (!p) document.querySelectorAll('input[name="rubric_positioning"]')[0].closest('.flex-col').classList.add('border', 'border-red-500', 'p-2', 'rounded');
                if (!pa) document.querySelectorAll('input[name="rubric_pacing"]')[0].closest('.flex-col').classList.add('border', 'border-red-500', 'p-2', 'rounded');
                
                setTimeout(() => {
                    document.querySelectorAll('.border-red-500').forEach(el => el.classList.remove('border', 'border-red-500', 'p-2', 'rounded'));
                }, 2000);
                
                return; // block advancement
            }
        }

        if (currentStepIndex < simSteps.length - 1) {'''

if "Validation for Slide 31" not in text:
    text = re.sub(pattern_next, replacement_next, text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
