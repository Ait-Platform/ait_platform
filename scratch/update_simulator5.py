import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# 1. Flatten app-view-32
old_view_32 = '''<div class="app-view hidden overflow-y-auto pb-20" id="app-view-32">
    <div class="bg-indigo-600 text-white p-4 rounded-t-xl mb-4 text-center">
      <h3 class="text-xl font-bold mb-1"><i class="fas fa-tasks mr-2"></i>Classroom Application</h3>
      <p class="text-xs text-indigo-200">Classroom Application</p>
    </div>
  
    <div class="bg-white p-4 rounded-lg shadow-sm border border-slate-200 mb-6">
      <h4 class="font-bold text-slate-800 mb-2 border-b pb-2">Practical & Oral Participation</h4>
      <p class="text-xs text-slate-500 mb-4">Check all that are successfully included in the participant's practical/oral participation.</p>'''

new_view_32 = '''<div class="app-view hidden overflow-y-auto pb-20" id="app-view-32">
    <div class="p-4 mb-2">
      <h3 class="text-xl font-bold mb-1 text-slate-800"><i class="fas fa-tasks text-indigo-500 mr-2"></i>Classroom Application</h3>
      <p class="text-xs text-slate-500 border-b pb-4">Check all that are successfully included in the participant's practical/oral participation.</p>
    </div>
  
    <div class="bg-white px-4 pb-4">'''

text = text.replace(old_view_32, new_view_32)

# 2. Remove buttons inside app-view-31 and app-view-32
text = text.replace('<button class="w-full py-4 bg-indigo-600 text-white font-bold rounded-lg shadow-lg hover:bg-indigo-700 transition" onclick="nextStep()">Proceed to Next Evaluation <i class="fas fa-arrow-right ml-2"></i></button>', '')
text = text.replace('<button class="w-full py-4 bg-indigo-600 text-white font-bold rounded-lg shadow-lg hover:bg-indigo-700 transition" onclick="nextStep()">Proceed to Post-Test <i class="fas fa-arrow-right ml-2"></i></button>', '')

# 3. Add Slide 32 Validation
old_next = '''                setTimeout(() => {
                    document.querySelectorAll('.border-red-500').forEach(el => el.classList.remove('border', 'border-red-500', 'p-2', 'rounded'));
                }, 2000);
                
                return; // block advancement
            }
        }

        if (currentStepIndex < simSteps.length - 1) {'''

new_next = '''                setTimeout(() => {
                    document.querySelectorAll('.border-red-500').forEach(el => el.classList.remove('border', 'border-red-500', 'p-2', 'rounded'));
                }, 2000);
                
                return; // block advancement
            }
        }
        
        // Validation for Slide 32
        if (currentStepData && currentStepData.appView === 32) {
            const checks = document.querySelectorAll('.application-check');
            let allChecked = true;
            checks.forEach(c => {
                if (!c.checked) {
                    allChecked = false;
                    c.closest('label').classList.add('border-red-500', 'bg-red-50');
                }
            });
            
            if (!allChecked) {
                alert("Please tick all the checkboxes to confirm successful inclusion before proceeding.");
                
                setTimeout(() => {
                    document.querySelectorAll('.border-red-500').forEach(el => el.classList.remove('border-red-500', 'bg-red-50'));
                }, 2000);
                
                return; // block advancement
            }
        }

        if (currentStepIndex < simSteps.length - 1) {'''

if "Validation for Slide 32" not in text:
    text = text.replace(old_next, new_next)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
