import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

# Modify showStep to change button text to Skip
old_show = """    if (step === TOTAL_STEPS) {
      document.getElementById('btn-next').classList.add('hidden');
      document.getElementById('btn-save').classList.remove('hidden');
    } else {
      document.getElementById('btn-next').classList.remove('hidden');
      document.getElementById('btn-save').classList.add('hidden');
    }"""

new_show = """    if (step === TOTAL_STEPS) {
      document.getElementById('btn-next').classList.add('hidden');
      document.getElementById('btn-save').classList.remove('hidden');
    } else {
      document.getElementById('btn-next').classList.remove('hidden');
      document.getElementById('btn-save').classList.add('hidden');
      
      const nextBtn = document.getElementById('btn-next');
      let isSkip = false;
      if (step === 3 && EXPECTED_BULK_WATER === 0) isSkip = true;
      if (step === 4 && EXPECTED_BULK_ELEC === 0) isSkip = true;
      if (step === 5 && EXPECTED_SUB_WATER === 0 && EXPECTED_SUB_ELEC === 0) isSkip = true;
      
      if (isSkip) {
        nextBtn.innerHTML = 'Skip <i class="fas fa-forward ml-2"></i>';
      } else {
        nextBtn.innerHTML = 'Next Step <i class="fas fa-arrow-right ml-2"></i>';
      }
    }"""

if 'isSkip = true' not in html:
    html = html.replace(old_show, new_show)
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print("Updated showStep successfully")
else:
    print("Already updated showStep")
