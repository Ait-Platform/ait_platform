import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_badges = """        <div class="flex space-x-2 text-[11px]">
          <span id="step-1-badge" class="px-2 py-1 rounded-full font-bold bg-blue-100 text-blue-700">1. Accounts</span>
          <span id="step-2-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">2. Bulk Meters</span>
          <span id="step-3-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">3. Sub Meters</span>
          <span id="step-4-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">4. Exceptions</span>
          <span id="step-5-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">5. Mapping</span>
        </div>"""

new_badges = """        <div class="flex space-x-2 text-[11px]">
          <span id="step-1-badge" class="px-2 py-1 rounded-full font-bold bg-blue-100 text-blue-700">1. Property</span>
          <span id="step-2-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">2. Accounts</span>
          <span id="step-3-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">3. Bulk Water</span>
          <span id="step-4-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">4. Bulk Elec</span>
          <span id="step-5-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">5. Sub Meters</span>
          <span id="step-6-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">6. Exceptions</span>
          <span id="step-7-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400">7. Mapping</span>
        </div>"""

if old_badges in content:
    content = content.replace(old_badges, new_badges)
else:
    print("WARNING: Could not find old badges exact string.")

# ALSO, we need to update the showStep() Javascript to handle the badge toggling!
old_showstep = """  function showStep(step) {
    document.querySelectorAll('.wizard-step').forEach(el => el.classList.add('hidden'));
    document.getElementById(`step-${step}`).classList.remove('hidden');
    
    document.getElementById('btn-prev').classList.toggle('hidden', step === 1);
    
    if (step === TOTAL_STEPS) {
      document.getElementById('btn-next').classList.add('hidden');
      document.getElementById('btn-save').classList.remove('hidden');
      buildMappingDashboard();
    } else {
      document.getElementById('btn-next').classList.remove('hidden');
      document.getElementById('btn-save').classList.add('hidden');
    }

    // Update badges
    for(let i=1; i<=TOTAL_STEPS; i++) {
      const b = document.getElementById(`step-${i}-badge`);
      if(b) {
        if(i === step) {
          b.className = "px-2 py-1 rounded-full font-bold bg-blue-100 text-blue-700";
        } else if (i < step) {
          b.className = "px-2 py-1 rounded-full font-bold bg-emerald-100 text-emerald-700";
        } else {
          b.className = "px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400";
        }
      }
    }
  }"""

# Actually showStep might just dynamically loop from 1 to TOTAL_STEPS.
# Let's check if the loop is `for(let i=1; i<=TOTAL_STEPS; i++)`. If so, I don't need to change JS!

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Fixed top badges in setup_wizard.html")
