import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# I need to completely replace showStep(step) with the correct version.
# Let's find it.
start_idx = content.find("  function showStep(step) {")
end_idx = content.find("  function validateGlobalMeters() {", start_idx)

if start_idx != -1 and end_idx != -1:
    old_show_step = content[start_idx:end_idx]
    
    new_show_step = """  function showStep(step) {
    document.querySelectorAll('.wizard-step').forEach(el => el.classList.add('hidden'));
    document.querySelectorAll('.step-content').forEach(el => el.classList.add('hidden'));
    
    const stepEl = document.getElementById(`step-${step}`);
    if (stepEl) stepEl.classList.remove('hidden');
    
    document.getElementById('btn-prev').classList.toggle('hidden', step === 1);
    
    if (step === TOTAL_STEPS) {
      document.getElementById('btn-next').classList.add('hidden');
      document.getElementById('btn-save').classList.remove('hidden');
    } else {
      document.getElementById('btn-next').classList.remove('hidden');
      document.getElementById('btn-save').classList.add('hidden');
    }

    if(step === 6) updateExceptionDropdowns();
    if(step === 7) buildMappingDashboard();
    if(step === 8) buildArrearsDashboard();
    if(step === 9) buildArrangementsDashboard();
    if(step === 10) buildOwnersDashboard();
    
    updateBadges();
  }

"""
    
    content = content.replace(old_show_step, new_show_step)
    
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Successfully replaced showStep.")
else:
    print("Could not find showStep.")
