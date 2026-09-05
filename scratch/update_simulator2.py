import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# 1. Flatten app-view-31 and remove defaults
old_view_31 = '''<div class="app-view hidden overflow-y-auto pb-20" id="app-view-31">
    <div class="bg-indigo-600 text-white p-4 rounded-t-xl mb-4 text-center">
      <h3 class="text-xl font-bold mb-1"><i class="fas fa-clipboard-check mr-2"></i>Evaluation of SACE Program</h3>
      <p class="text-xs text-indigo-200">Evaluation of Facilitator</p>
    </div>
    
    <div class="bg-white p-4 rounded-lg shadow-sm border border-slate-200 mb-6">
      <h4 class="font-bold text-slate-800 mb-2 border-b pb-2">Practical Rubric</h4>
      <p class="text-xs text-slate-500 mb-4">Rate the physical demonstration of the LITRE sequence (0: Not demonstrated, 3: Clear/Strong)</p>'''

new_view_31 = '''<div class="app-view hidden overflow-y-auto pb-20" id="app-view-31">
    <div class="p-4 mb-2">
      <h3 class="text-xl font-bold mb-1 text-slate-800"><i class="fas fa-clipboard-check text-indigo-500 mr-2"></i>Evaluation of Facilitator</h3>
      <p class="text-xs text-slate-500 border-b pb-4">Rate the physical demonstration of the LITRE sequence (0: Not demonstrated, 3: Clear/Strong)</p>
    </div>
    
    <div class="bg-white px-4 pb-4">'''

text = text.replace(old_view_31, new_view_31)

# Remove the 'checked' attribute and reset class for the default radio buttons
bad_checked = 'class="flex-1 text-center py-2 bg-indigo-100 border border-indigo-300 rounded cursor-pointer"><input type="radio" name="rubric_vocalization" value="3" checked class="mr-1"> 3</label>'
good_checked = 'class="flex-1 text-center py-2 bg-slate-50 border border-slate-200 rounded cursor-pointer hover:bg-indigo-50"><input type="radio" name="rubric_vocalization" value="3" class="mr-1"> 3</label>'
text = text.replace(bad_checked, good_checked)

bad_checked2 = 'class="flex-1 text-center py-2 bg-indigo-100 border border-indigo-300 rounded cursor-pointer"><input type="radio" name="rubric_positioning" value="3" checked class="mr-1"> 3</label>'
good_checked2 = 'class="flex-1 text-center py-2 bg-slate-50 border border-slate-200 rounded cursor-pointer hover:bg-indigo-50"><input type="radio" name="rubric_positioning" value="3" class="mr-1"> 3</label>'
text = text.replace(bad_checked2, good_checked2)

bad_checked3 = 'class="flex-1 text-center py-2 bg-indigo-100 border border-indigo-300 rounded cursor-pointer"><input type="radio" name="rubric_pacing" value="3" checked class="mr-1"> 3</label>'
good_checked3 = 'class="flex-1 text-center py-2 bg-slate-50 border border-slate-200 rounded cursor-pointer hover:bg-indigo-50"><input type="radio" name="rubric_pacing" value="3" class="mr-1"> 3</label>'
text = text.replace(bad_checked3, good_checked3)

# 2. Add validation to nextStep()
old_next = '''    function nextStep() {
        if (currentStepIndex < simSteps.length - 1) {
            currentStepIndex++;
            applyStep();
        } else {'''

new_next = '''    function nextStep() {
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

        if (currentStepIndex < simSteps.length - 1) {
            currentStepIndex++;
            applyStep();
        } else {'''

if "Validation for Slide 31" not in text:
    text = text.replace(old_next, new_next)

# 3. Change "SACE Auditor" to "Provider Auditor"
text = text.replace('SACE Auditor (A)', 'Provider Auditor (A)')
text = text.replace('TAB A: SACE AUDITOR', 'TAB A: PROVIDER AUDITOR')

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
