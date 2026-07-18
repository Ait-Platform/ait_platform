with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

# 1. Swap Badges
old_badges = """          <span id="step-10-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400 whitespace-nowrap">10. Rates</span>
          <span id="step-11-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400 whitespace-nowrap">11. Arrangements</span>"""
new_badges = """          <span id="step-10-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400 whitespace-nowrap">10. Arrangements</span>
          <span id="step-11-badge" class="px-2 py-1 rounded-full font-bold bg-slate-100 text-slate-400 whitespace-nowrap">11. Rates</span>"""
html = html.replace(old_badges, new_badges)

# 2. Swap Step DIVs
old_step10 = """      <div id="step-10" class="wizard-step hidden">
        <h2 class="text-xl font-bold text-slate-800 mb-2">Step 10: Rates</h2>
        <p class="text-sm text-slate-600 mb-6">Enter any rates for each account (if applicable).</p>
        <div id="rates-container" class="space-y-3"></div>
      </div>"""
new_step10 = """      <div id="step-10" class="wizard-step hidden">
        <h2 class="text-xl font-bold text-slate-800 mb-2">Step 10: Payment Arrangements</h2>
        <p class="text-sm text-slate-600 mb-6">Enter any payment arrangements (amount and duration in months) for each account.</p>
        <div id="arrangements-container" class="space-y-3"></div>
      </div>"""

old_step11 = """      <div id="step-11" class="wizard-step hidden">
        <h2 class="text-xl font-bold text-slate-800 mb-2">Step 11: Payment Arrangements</h2>
        <p class="text-sm text-slate-600 mb-6">Enter any payment arrangements (amount and duration in months) for each account.</p>
        <div id="arrangements-container" class="space-y-3"></div>
      </div>"""
new_step11 = """      <div id="step-11" class="wizard-step hidden">
        <h2 class="text-xl font-bold text-slate-800 mb-2">Step 11: Rates</h2>
        <p class="text-sm text-slate-600 mb-6">Enter any rates for each account (if applicable).</p>
        <div id="rates-container" class="space-y-3"></div>
      </div>"""

html = html.replace(old_step10, new_step10)
html = html.replace(old_step11, new_step11)

# 3. Swap JS logic in showStep
old_show = """    if(step === 10) buildRatesDashboard();
    if(step === 11) buildArrangementsDashboard();"""
new_show = """    if(step === 10) buildArrangementsDashboard();
    if(step === 11) buildRatesDashboard();"""
html = html.replace(old_show, new_show)

# 4. Swap JS logic in nextStep
old_next = """    if (currentStep === 10) { if (!gatherRates()) return; }
    if (currentStep === 11) { if (!gatherArrangements()) return; }"""
new_next = """    if (currentStep === 10) { if (!gatherArrangements()) return; }
    if (currentStep === 11) { if (!gatherRates()) return; }"""
html = html.replace(old_next, new_next)

# 5. Fix labels array
old_labels = """const labels = ['1. Property', '2. Accounts', '3. Bulk Water', '4. Bulk Elec', '5. Sub Meters', '6. Exceptions', '7. Mapping', '8. Readings', '9. Arrears', '10. Arrangements', '11. Owners'];"""
new_labels = """const labels = ['1. Property', '2. Accounts', '3. Bulk Water', '4. Bulk Elec', '5. Sub Meters', '6. Exceptions', '7. Mapping', '8. Readings', '9. Arrears', '10. Arrangements', '11. Rates', '12. Owners'];"""
html = html.replace(old_labels, new_labels)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(html)
print("Swapped Steps 10 and 11 successfully.")
